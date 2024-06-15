package main

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	projectID = "sharechat-production"

	topic_topic_chat_mem_instance = "production-spanner-3"
	topic_topic_chat_mem_database = "production-db"

	sourceInstanceID = "topic-cdc-test-instance"
	sourceDatabaseID = "sharechat"

	// TODO
	pubsubSubscription = "topic-table-ids-subscription"
)

var maxParallelismForConsumingMessages int
var maxParallelismForProcessingEachMessage int
var maxMutationsPerBatch int

func main() {
	ctx := context.Background()

	// Create a new SpannerClient for topic and topic_chat_member database
	topicAndTopicChatMemberClient, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, topic_topic_chat_mem_instance, topic_topic_chat_mem_database))
	if err != nil {
		log.Fatalf("Failed to create client 1: %v", err)
	}
	defer topicAndTopicChatMemberClient.Close()

	// backup topic table
	topicBackupClient, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, sourceInstanceID, sourceDatabaseID))
	if err != nil {
		log.Fatalf("Failed to create source client: %v", err)
	}
	defer topicBackupClient.Close()

	// Read messages from Pub/Sub
	// Create a new PubSubClient
	pubsubclient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer pubsubclient.Close()

	maxParallelistForConsumingMessagesAsString := os.Getenv("MAX_PARALLELISM_FOR_CONSUMING_MESSAGES")

	if maxParallelistForConsumingMessagesAsString == "" {
		maxParallelismForConsumingMessages = 10
	} else {
		maxParallelismForConsumingMessages, _ = strconv.Atoi(maxParallelistForConsumingMessagesAsString)
	}

	maxParallelistForProcessingEachMessageAsString := os.Getenv("MAX_PARALLELISM_FOR_PROCESSING_EACH_MESSAGE")
	if maxParallelistForProcessingEachMessageAsString == "" {
		maxParallelismForProcessingEachMessage = 10
	} else {
		maxParallelismForProcessingEachMessage, _ = strconv.Atoi(maxParallelistForConsumingMessagesAsString)
	}

	maxMutationsPerBatchAsString := os.Getenv("MAX_MUTATIONS_PER_BATCH")
	if maxMutationsPerBatchAsString == "" {
		maxMutationsPerBatch = 1000
	} else {
		maxMutationsPerBatch, _ = strconv.Atoi(maxMutationsPerBatchAsString)

	}

	// Get the subscription
	sub := pubsubclient.Subscription(pubsubSubscription)

	// Use a WaitGroup to wait for all message processing to complete
	var wg sync.WaitGroup

	numberOfMessagesChannel := make(chan struct{}, maxParallelismForConsumingMessages)

	// Receive messages
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		wg.Add(1)
		numberOfMessagesChannel <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() {
				<-numberOfMessagesChannel
			}()

			// Process the message
			var topicIds []string
			err := json.Unmarshal(msg.Data, &topicIds)
			if err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				return
			}
			processMessage(ctx, topicAndTopicChatMemberClient, topicBackupClient, topicIds)
			// Acknowledge the message
			msg.Ack()
		}()
	})

	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

func processMessage(ctx context.Context, topicAndTopicChatMemberClient, topicBackupClient *spanner.Client, topicIds []string) {
	// Batch process the topicIds in different goroutines
	var wg sync.WaitGroup
	numberOfTopicsChannel := make(chan struct{}, maxParallelismForProcessingEachMessage)
	for _, topicID := range topicIds {
		wg.Add(1)
		numberOfTopicsChannel <- struct{}{}
		go func(topicID string) {
			defer wg.Done()
			defer func() {
				<-numberOfTopicsChannel
			}()

			// Double check again if this topic exists in source topic table or not
			// delete only if it doesn't exist
			if !checkIfTopicExistsInSourceTopicTable(ctx, topicAndTopicChatMemberClient, topicID) {
				// Process the topic
				deleteChildTables(ctx, topicAndTopicChatMemberClient, topicID)
			}

			// Delete from the source backup topic table
			mutationsTopic := []*spanner.Mutation{
				spanner.Delete("topic", spanner.Key{topicID}),
			}

			// Apply the mutations
			_, errTopic := topicBackupClient.Apply(ctx, mutationsTopic)
			if errTopic != nil {
				log.Printf("failed to delete from topic table: %v", errTopic)
			}

		}(topicID)
	}
	wg.Wait()
}

func checkIfTopicExistsInSourceTopicTable(ctx context.Context, topicAndTopicChatMemberClient *spanner.Client, topicId string) bool {

	stmt := spanner.Statement{
		SQL:    "SELECT topicId FROM topic WHERE topicId = @topicId",
		Params: map[string]interface{}{"topicId": topicId},
	}
	iter := topicAndTopicChatMemberClient.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: sppb.RequestOptions_PRIORITY_LOW})
	defer iter.Stop()

	if _, err := iter.Next(); errors.Is(err, iterator.Done) {
		log.Printf("Topic not found in topic for topidId=%s, hence deleting child data", topicId)
		return false
	} else {
		log.Printf("Topic found in topic for topidId=%s, this shouldn't have happened", topicId)
		return true
	}
}

func deleteChildTables(ctx context.Context, topicAndTopicChatMemberClient *spanner.Client, topicId string) {
	// topic_events ---> topic_event_topicId_status_scheduledAt_index
	// topic_chat_member ---> topic_chat_member_topicId_lastUpdatedAt_index
	_, err1 := topicAndTopicChatMemberClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		fmt.Printf("Deleting for topic_chat_member,topic_events for topicId:%s\n", topicId)
		statements := []spanner.Statement{
			{
				SQL:    "DELETE FROM topic_chat_member@{FORCE_INDEX=topic_chat_member_topicId_lastUpdatedAt_index} WHERE topicId = @topicId",
				Params: map[string]interface{}{"topicId": topicId},
			},
			{
				SQL:    "DELETE FROM topic_events@{FORCE_INDEX=topic_event_topicId_status_scheduledAt_index} WHERE topicId = @topicId",
				Params: map[string]interface{}{"topicId": topicId},
			},
		}

		for _, stmt := range statements {
			if _, err := txn.UpdateWithOptions(ctx, stmt, spanner.QueryOptions{Priority: sppb.RequestOptions_PRIORITY_LOW}); err != nil {
				return err
			}
		}
		return nil
	})
	if err1 != nil {
		if strings.Contains(fmt.Sprint(err1), "The transaction contains too many mutations") {
			fmt.Printf("Since number of records in topic_chat_member were too high for, doing a batch delete for topicId:%s", topicId)
			// purposefully ignoring the error
			batchDeleteTopicChatMember(ctx, topicAndTopicChatMemberClient, "topic_chat_member", "memberId", topicId, "topic_chat_member_topicId_lastUpdatedAt_index")
		}
	}
}

func batchDeleteTopicChatMember(ctx context.Context, client *spanner.Client, table, column, topicId, index string) error {
	// Get IDs to delete
	ids, err := getIdsToDeleteTopicChatMember(ctx, client, table, column, topicId, index)
	if err != nil {
		return err
	}

	// Split IDs into batches
	batches := splitIntoBatchesTopicChatMember(ids, maxMutationsPerBatch)

	for _, batch := range batches {
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			stmt := spanner.Statement{
				// TODO - make it generic
				SQL:    fmt.Sprintf("DELETE FROM %s@{FORCE_INDEX=%s} WHERE %s IN UNNEST(@ids) AND topicId='%s'", table, index, column, topicId),
				Params: map[string]interface{}{"ids": batch},
			}
			_, err := txn.UpdateWithOptions(ctx, stmt, spanner.QueryOptions{Priority: sppb.RequestOptions_PRIORITY_LOW})
			return err
		})
		if err != nil {
			// ignore
		}
	}

	return nil
}

func getIdsToDeleteTopicChatMember(ctx context.Context, client *spanner.Client, table, column, topicId, index string) ([]string, error) {
	stmt := spanner.Statement{
		SQL:    fmt.Sprintf("SELECT %s FROM %s WHERE topicId = @topicId", column, table),
		Params: map[string]interface{}{"topicId": topicId},
	}

	iter := client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: sppb.RequestOptions_PRIORITY_LOW})
	defer iter.Stop()

	var ids []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var id string
		if err := row.Columns(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func splitIntoBatchesTopicChatMember(ids []string, batchSize int) [][]string {
	var batches [][]string
	for batchSize < len(ids) {
		ids, batches = ids[batchSize:], append(batches, ids[0:batchSize:batchSize])
	}
	return append(batches, ids)
}
