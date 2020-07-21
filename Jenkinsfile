pipeline {
    agent {
        docker {
            image 'maven:3-alpine'
            label 'docker'
        }
    }
    stages {
        stage('build') {
            steps {
                sh 'mvn --version'
            }
        }
    }
}
