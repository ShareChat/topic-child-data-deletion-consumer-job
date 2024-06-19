pipeline {
  agent {
    kubernetes {
      label 'sc-live-topic-child-table-cleanup-consumer'
      yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: dind
    image: sc-mum-armory.platform.internal/devops/dind:v2
    securityContext:
      privileged: true
    env:
    - name: DOCKER_HOST
      value: tcp://localhost:2375
    - name: DOCKER_TLS_CERTDIR
      value: ""
    volumeMounts:
      - name: dind-storage
        mountPath: /var/lib/docker
    readinessProbe:
      tcpSocket:
        port: 2375
      initialDelaySeconds: 30
      periodSeconds: 10
    livenessProbe:
      tcpSocket:
        port: 2375
      initialDelaySeconds: 30
      periodSeconds: 20
  - name: builder
    image: sc-mum-armory.platform.internal/devops/builder-image-armory
    command:
    - sleep
    - infinity
    env:
    - name: DOCKER_HOST
      value: tcp://localhost:2375
    - name: DOCKER_BUILDKIT
      value: "0"
    volumeMounts:
      - name: jenkins-sa
        mountPath: /root/.gcp/
  volumes:
    - name: dind-storage
      emptyDir: {}
    - name: jenkins-sa
      secret:
        secretName: jenkins-sa
"""
    }
  }


  options {
    timeout(time: 10, unit: 'MINUTES')
    copyArtifactPermission('*')
  }

  environment {
    GITHUB_TOKEN = credentials('github-access')
    sc_regions="mumbai"
    app="sc-live-topic-table-database-cleanup-consumer"
    buildarg_DEPLOYMENT_ID="sc-live-topic-table-database-cleanup-consumer-$GIT_COMMIT"
    buildarg_GITHUB_TOKEN=credentials('github-access')
    JS_REGISTRY_TOKEN = credentials('JS_REGISTRY_TOKEN')
    buildarg_JS_REGISTRY_TOKEN = credentials('JS_REGISTRY_TOKEN')
  }

  stages {
      stage('build') {
        steps {
          container('builder') {
              sh 'armory build'
          }
        }
      }

      stage('push') {
        when {
          anyOf {
            branch 'main'
          }
        }
        steps {
          container('builder') {
            sh 'armory push'
          }
        }
      }
  }
}