pipeline {
    agent { label 'x86_64' }

    parameters {
        string(defaultValue: 'fixes20260712', name: 'BRANCH')
        string(defaultValue: '2026.3', name: 'VERSION')
        string(defaultValue: 'jenkins-maple', name: 'CREDENTIALS_ID')
        string(defaultValue: 'ssh://git.maple.maceroc.com/git/millegrilles.midcompte.python', name: 'GIT_URL')
        string(defaultValue: 'registry.millegrilles.com:5000/millegrilles/midcompte_python', name: 'DOCKER_IMAGE')
    }

    environment {
        NOM_APP="millegrilles_messages"
        VBUILD="${VERSION}.${BUILD_NUMBER}"
        DOCKER_IMAGE="${params.DOCKER_IMAGE}"
    }

    stages {
        stage('docker build x86_64') {
            steps {
                checkout scmGit(branches: [[name: params.BRANCH]], extensions: [submodule(recursiveSubmodules: true, reference: '')], userRemoteConfigs: [[credentialsId: params.CREDENTIALS_ID, url: params.GIT_URL]])

                sh '''
                # Creer image docker
                docker build -t ${DOCKER_IMAGE}:${VBUILD} .
                docker push ${DOCKER_IMAGE}:${VBUILD}
                '''
            }
        }
    }
}
