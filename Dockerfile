# Imagem base com Java e Spark instalados
FROM openjdk:8-jdk

# Instalação do Spark
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.3.1-bin-hadoop3.tgz && \
    mv spark-3.3.1-bin-hadoop3 /opt/spark && \
    rm spark-3.3.1-bin-hadoop3.tgz

# Variáveis de ambiente para o Spark
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos do projeto para o contêiner
COPY . /app

# Compila o projeto
RUN apt-get install -y maven && mvn clean package

RUN ls /app/target

# Define o comando de entrada para rodar o código
CMD ["spark-submit", "--class", "LogAnalysis", "--master", "local[*]", "/app/target/spark-java-app-1.0-SNAPSHOT.jar"]
