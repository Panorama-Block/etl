FROM python:3.10-slim

# Instalação de dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Instalação do Poetry
RUN pip3 install --no-cache-dir poetry

# Configuração do Spark
ENV SPARK_VERSION=3.4.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download e instalação do Spark
RUN curl -L --retry 3 --retry-delay 5 \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Baixar jars necessários para acessar o MinIO via S3A
ENV HADOOP_VERSION=3.3.2
ENV AWS_SDK_VERSION=1.12.262
ENV GUAVA_VERSION=31.1-jre

RUN mkdir -p ${SPARK_HOME}/jars && \
    curl -L --retry 3 --retry-delay 5 -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L --retry 3 --retry-delay 5 -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -L --retry 3 --retry-delay 5 -o ${SPARK_HOME}/jars/guava-${GUAVA_VERSION}.jar \
    https://repo1.maven.org/maven2/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar

# Baixar o driver JDBC do ClickHouse
ENV CLICKHOUSE_JDBC_VERSION=0.6.0

RUN curl -L --retry 3 --retry-delay 5 -o ${SPARK_HOME}/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar

ENV SPARK_EXTRA_CLASSPATH=${SPARK_HOME}/jars/*

# Diretório de trabalho
WORKDIR /app

# Set Python path
ENV PYTHONPATH=/app

# Configure pip and poetry for better reliability
RUN pip3 config set global.timeout 1000 && \
    pip3 config set global.retries 10 && \
    poetry config virtualenvs.create false && \
    poetry config installer.max-workers 10

# Instala dependências Python
COPY pyproject.toml poetry.lock* ./
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=cache,target=/root/.cache/pypoetry \
    poetry install --no-interaction --no-ansi --no-root

# Copia código-fonte
COPY src/pipeline ./pipeline

# Create directory for scripts
RUN mkdir -p /opt/scripts

# Create startup script
RUN echo '#!/bin/bash\n\
# Print environment variables for debugging\n\
echo "=== Environment Variables ==="\n\
echo "MINIO_URL: ${MINIO_URL}"\n\
echo "MINIO_USER: ${MINIO_USER}"\n\
echo "MINIO_PASSWORD: ${MINIO_PASSWORD}"\n\
echo "MINIO_BUCKET: ${MINIO_BUCKET}"\n\
echo "HADOOP_VERSION: ${HADOOP_VERSION}"\n\
echo "AWS_SDK_VERSION: ${AWS_SDK_VERSION}"\n\
echo "GUAVA_VERSION: ${GUAVA_VERSION}"\n\
\n\
# Verify S3A jars exist\n\
echo "=== Checking S3A Jars ==="\n\
ls -l ${SPARK_HOME}/jars/hadoop-aws-*.jar\n\
ls -l ${SPARK_HOME}/jars/aws-java-sdk-bundle-*.jar\n\
ls -l ${SPARK_HOME}/jars/guava-*.jar\n\
\n\
# Start Kafka consumers in the background\n\
poetry run python -m pipeline.app &\n\
KAFKA_PID=$!\n\
\n\
# Wait for Kafka to be ready\n\
sleep 10\n\
\n\
# Start Spark job with proper configuration\n\
/opt/spark/bin/spark-submit \\\n\
    --master local[*] \\\n\
    --conf spark.hadoop.fs.s3a.endpoint=${MINIO_URL} \\\n\
    --conf spark.hadoop.fs.s3a.access.key=${MINIO_USER} \\\n\
    --conf spark.hadoop.fs.s3a.secret.key=${MINIO_PASSWORD} \\\n\
    --conf spark.hadoop.fs.s3a.path.style.access=true \\\n\
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\\n\
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\\n\
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\\n\
    --conf spark.hadoop.fs.s3a.buffer.dir=/tmp \\\n\
    --conf spark.hadoop.fs.s3a.committer.name=directory \\\n\
    --conf spark.sql.parquet.mergeSchema=false \\\n\
    --conf spark.sql.parquet.filterPushdown=true \\\n\
    --conf spark.sql.parquet.recordLevelFilter.enabled=true \\\n\
    --conf spark.sql.parquet.columnarReaderBatchSize=4096 \\\n\
    --conf spark.driver.extraJavaOptions="-Dcom.amazonaws.services.s3.enableV4=true" \\\n\
    --conf spark.executor.extraJavaOptions="-Dcom.amazonaws.services.s3.enableV4=true" \\\n\
    --conf spark.hadoop.fs.s3a.aws.region=us-east-1 \\\n\
    --conf spark.hadoop.fs.s3a.aws.credentials.provider.path=/tmp \\\n\
    --conf spark.hadoop.fs.s3a.aws.credentials.provider.impl=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\\n\
    --conf spark.driver.extraClassPath=${SPARK_HOME}/jars/* \\\n\
    --conf spark.executor.extraClassPath=${SPARK_HOME}/jars/* \\\n\
    /app/pipeline/core/spark_job.py\n\
\n\
# Wait for Kafka process\n\
wait $KAFKA_PID\n\
' > /opt/scripts/start.sh && chmod +x /opt/scripts/start.sh

# Set environment variables
ENV SPARK_MASTER=local[*]
ENV MINIO_URL=minio:9000
ENV MINIO_USER=admin
ENV MINIO_PASSWORD=admin123
ENV MINIO_BUCKET=data
ENV AWS_ACCESS_KEY_ID=${MINIO_USER}
ENV AWS_SECRET_ACCESS_KEY=${MINIO_PASSWORD}
ENV AWS_DEFAULT_REGION=us-east-1

# Use the startup script as entrypoint
ENTRYPOINT ["/opt/scripts/start.sh"]