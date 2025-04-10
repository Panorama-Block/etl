FROM bitnami/spark:latest

# Instalação do Python e Poetry
USER root
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    pip3 install poetry && \
    ln -s /usr/bin/python3 /usr/bin/python

# Baixar jars necessários para acessar o MinIO via S3A
ENV HADOOP_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901

RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L --retry 3 --retry-delay 5 -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L --retry 3 --retry-delay 5 -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Baixar o driver JDBC do ClickHouse
ENV CLICKHOUSE_JDBC_VERSION=0.6.0

RUN curl -L --retry 3 --retry-delay 5 -o /opt/bitnami/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar

ENV SPARK_EXTRA_CLASSPATH=/opt/bitnami/spark/jars/*

# Diretório de trabalho
WORKDIR /pipeline

# Instala dependências Python
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root

# Copia código-fonte
COPY . .

# Exposição da porta (caso use Flask, etc)
EXPOSE 5000

# Comando principal
CMD ["poetry", "run", "python", "app.py"]