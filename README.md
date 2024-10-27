# zazmic_interview

mvn clean package
docker build -t spark-java-app .
docker run --name spark-java-app-log spark-java-app
