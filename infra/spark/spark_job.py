import argparse
import logging
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct

logging.basicConfig(level=logging.INFO)

def process_file(file_name, bucket, minio_url, minio_user, minio_password, clickhouse_jdbc_url):
    try:
        spark = SparkSession.builder.appName("MinioToClickhouseJob").getOrCreate()

        # Configure Hadoop for MinIO access
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", minio_url)
        hadoop_conf.set("fs.s3a.access.key", minio_user)
        hadoop_conf.set("fs.s3a.secret.key", minio_password)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

        file_path = f"s3a://{bucket}/{file_name}"
        logging.info(f"Reading file: {file_path}")
        df = spark.read.parquet(file_path)
        logging.info("File read successfully.")

        df_transformed = df.withColumn(
            "networkToken",
            struct(
                col("networkToken.name").alias("name"),
                col("networkToken.symbol").alias("symbol"),
                col("networkToken.decimals").alias("decimals"),
                col("networkToken.logoUri").alias("logoUri"),
                col("networkToken.description").alias("description")
            )
        )

        logging.info("Writing data to ClickHouse...")
        connection_properties = {"driver": "com.clickhouse.jdbc.ClickHouseDriver"}
        df_transformed.write.mode("append").jdbc(url=clickhouse_jdbc_url, table="chains", properties=connection_properties)
        logging.info("Data written successfully.")

    except Exception as e:
        logging.error("Error in spark_job.py: " + str(e))
        logging.error(traceback.format_exc())
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark job to process Parquet file from MinIO and load into ClickHouse")
    parser.add_argument("--file", required=True, help="Parquet file name to process")
    parser.add_argument("--bucket", required=True, help="MinIO bucket name")
    parser.add_argument("--minio-url", required=True, help="MinIO endpoint (e.g., http://minio:9000)")
    parser.add_argument("--minio-user", required=True, help="MinIO access key")
    parser.add_argument("--minio-password", required=True, help="MinIO secret key")
    parser.add_argument("--clickhouse-jdbc", required=True, help="JDBC URL for ClickHouse (e.g., jdbc:clickhouse://clickhouse:8123/default)")
    args = parser.parse_args()

    process_file(args.file, args.bucket, args.minio_url, args.minio_user, args.minio_password, args.clickhouse_jdbc)
