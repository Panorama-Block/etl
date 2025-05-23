from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, array
from pyspark.sql.types import StringType, ArrayType, StructType, BooleanType, IntegerType
import json
import logging

logging.basicConfig(level=logging.INFO)

def run_spark_chains(
        file_name: str,
        bucket: str,
        minio_url: str,
        minio_user: str,
        minio_password: str,
        clickhouse_url: str,
        clickhouse_table: str, 
        clickhouse_user: str = "default",
        clickhouse_password: str = ""
    ) -> tuple:
    try:

        # Initialize a SparkSession
        spark = SparkSession.builder \
            .appName("MeuApp") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logging.info(f"Error initializing Spark session: {e}")
        return None, []

    # Print the Spark version to confirm connection
    spark_version = spark.version
    logging.info(f"Spark version: {spark_version}")

    # Build full S3A path to parquet file
    s3_path = f"s3a://{bucket}/{file_name}"

    # Read the parquet file from MinIO
    try:
        df = spark.read.parquet(s3_path)
        df.show()

        # Get the list of columns
        columns = df.columns

        logging.info(f"Columns in the DataFrame: {df.first()}")

        df = sanitize_for_jdbc(df)

        logging.info(f"DataFrame schema after sanitization: {df.printSchema()}")

        # Print the first line of the DataFrame
        logging.info(f"First line of the DataFrame: {df.first()}")

        df = df.select(
            "chainId", "status", "chainName", "description", "platformChainId", "subnetId",
            "vmId", "vmName", "explorerUrl", "rpcUrl", "wsUrl",
            "isTestnet", "private", "chainLogoUri", "enabledFeatures",
            "networkToken_name", "networkToken_symbol", "networkToken_decimals", "networkToken_logoUri", "networkToken_description"
        )


        logging.info(f"DataFrame schema after selecting columns: {df.first()}")

        df.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", clickhouse_table) \
            .option("user", clickhouse_user) \
            .option("password", clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
        
        logging.info(f"Data written to ClickHouse table {clickhouse_table} successfully.")
        
    except Exception as e:
        logging.info(f"Error reading or processing the parquet file: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def sanitize_for_jdbc(df):
    # Expande os campos de networkToken
    df = df.withColumn("networkToken_name", col("networkToken.name")) \
           .withColumn("networkToken_symbol", col("networkToken.symbol")) \
           .withColumn("networkToken_decimals", col("networkToken.decimals").cast("int")) \
           .withColumn("networkToken_logoUri", col("networkToken.logoUri")) \
           .withColumn("networkToken_description", col("networkToken.description")) \
           .drop("networkToken")

    # Sanitiza campo enabledFeatures (converte array<string> em string JSON)
    array_to_json_udf = udf(lambda x: json.dumps(x) if x else "[]")
    df = df.withColumn(
        "enabledFeatures",
        when(col("enabledFeatures").isNull(), "[]").otherwise(array_to_json_udf(col("enabledFeatures")))
    )

    # Converte booleanos para inteiros (UInt8 no ClickHouse)
    for boolean_field in ["isTestnet", "private"]:
        df = df.withColumn(boolean_field, when(col(boolean_field) == True, 1).otherwise(0))

    return df

def run_spark_blocks(
        file_name: str,
        bucket: str,
        minio_url: str,
        minio_user: str,
        minio_password: str,
        clickhouse_url: str,
        clickhouse_table: str, 
        clickhouse_user: str = "default",
        clickhouse_password: str = ""
    ) -> tuple:
    try:

        # Initialize a SparkSession
        spark = SparkSession.builder \
            .appName("MeuApp") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logging.info(f"Error initializing Spark session: {e}")
        return None, []
    
    # Print the Spark version to confirm connection
    spark_version = spark.version
    logging.info(f"Spark version: {spark_version}")

    # Build full S3A path to parquet file
    s3_path = f"s3a://{bucket}/{file_name}"

    # Read the parquet file from MinIO
    try:
        df = spark.read.parquet(s3_path)
        df.show()

        # Get the list of columns
        columns = df.columns

        logging.info(f"Columns in the DataFrame: {df.first()}")

        
        
    except Exception as e:
        logging.info(f"Error reading or processing the parquet file: {e}")
        columns = []

    spark.stop()
    return spark_version, columns


def clean_blocks(df):
    pass 