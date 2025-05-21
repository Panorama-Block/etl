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
ENV HADOOP_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901

RUN mkdir -p ${SPARK_HOME}/jars && \
    curl -L --retry 3 --retry-delay 5 -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L --retry 3 --retry-delay 5 -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

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

# Exposição da porta (caso use Flask, etc)
EXPOSE 5000

# Comando principal
CMD ["poetry", "run", "python", "-m", "pipeline.app"]