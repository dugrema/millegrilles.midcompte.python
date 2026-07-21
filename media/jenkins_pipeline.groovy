pipeline {
    agent { label 'x86_64' }

    parameters {
        string(defaultValue: 'fixes20260712', name: 'BRANCH')
        string(defaultValue: '2026.3', name: 'VERSION')
        string(defaultValue: 'jenkins-maple', name: 'CREDENTIALS_ID')
        string(defaultValue: 'ssh://git.maple.maceroc.com/git/millegrilles.midcompte.python', name: 'GIT_URL')
        string(defaultValue: 'registry.millegrilles.com:5000/millegrilles/media_python', name: 'DOCKER_IMAGE')
    }

    environment {
        VBUILD="${VERSION}.${BUILD_NUMBER}"
        DOCKER_IMAGE="${params.DOCKER_IMAGE}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scmGit(branches: [[name: params.BRANCH]], extensions: [], userRemoteConfigs: [[credentialsId: params.CREDENTIALS_ID, url: params.GIT_URL]])
            }
        }

        stage('Build & Package & Deploy') {
            steps {
                sh "cd media && make deploy VERSION_FULL=${VBUILD}"
            }
        }
    }

    post {
        success {
            archiveArtifacts artifacts: 'artifacts/', followSymlinks: false
        }
    }
}
