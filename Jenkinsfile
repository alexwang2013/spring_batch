pipeline {
    agent {
        docker {
            image 'maven'
            args '-v /Users/wang/.m2:/root/.m2'
        }
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn -B -DskipTests clean package'
            }
        }
    }
}