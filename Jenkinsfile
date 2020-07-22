pipeline {
    agent {
        docker {
            image 'maven:3.3-jdk-8'
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
