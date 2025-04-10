from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType, StructType

def run_spark_job(
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
        print(f"Error initializing Spark session: {e}")
        return None, []

    # Print the Spark version to confirm connection
    spark_version = spark.version
    print(f"Spark version: {spark_version}")

    # Build full S3A path to parquet file
    s3_path = f"s3a://{bucket}/{file_name}"

    # Read the parquet file from MinIO
    try:
        df = spark.read.parquet(s3_path)
        df.show()

        # Get the list of columns
        columns = df.columns

        df = sanitize_for_jdbc(df)

        df.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", clickhouse_table) \
            .option("user", clickhouse_user) \
            .option("password", clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
        
        print(f"Data written to ClickHouse table {clickhouse_table} successfully.")
        
    except Exception as e:
        print(f"Error reading or processing the parquet file: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def sanitize_for_jdbc(df):
    array_to_str = udf(lambda x: ','.join(x) if x else None, StringType())
    struct_to_str = udf(lambda x: str(x.asDict()) if x else None, StringType())

    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, array_to_str(col(field.name)))
        elif isinstance(field.dataType, StructType):
            df = df.withColumn(field.name, struct_to_str(col(field.name)))
    return df

