import logging
import sys
import time
import os
from pyspark.sql import SparkSession
from .client_clickhouse import spark_clickhouse_run

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('spark_job.log')
    ]
)

logger = logging.getLogger(__name__)

def log_spark_config(spark):
    """Log all Spark configurations related to S3A"""
    logger.info("=== Spark Configuration ===")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    # Log all S3A related configurations
    s3a_configs = [
        "fs.s3a.endpoint",
        "fs.s3a.access.key",
        "fs.s3a.secret.key",
        "fs.s3a.impl",
        "fs.s3a.path.style.access",
        "fs.s3a.connection.ssl.enabled",
        "fs.s3a.aws.credentials.provider"
    ]
    
    for config in s3a_configs:
        value = hadoop_conf.get(config)
        if config.endswith("secret.key"):
            value = "***" if value else None
        logger.info(f"{config}: {value}")

def test_s3_connection(spark):
    """Test S3 connection by listing the bucket contents"""
    try:
        logger.info("=== Testing S3 Connection ===")
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        bucket = os.getenv("MINIO_BUCKET", "data")
        endpoint = os.getenv("MINIO_URL", "http://minio:9000")
        
        logger.info(f"Attempting to list contents of bucket: {bucket}")
        logger.info(f"Using endpoint: {endpoint}")
        
        # Try to list files in the bucket
        files = spark.sparkContext.wholeTextFiles(f"s3a://{bucket}/*")
        count = files.count()
        logger.info(f"Successfully listed {count} files in bucket")
        
        # Log first few files found
        for path, _ in files.take(5):
            logger.info(f"Found file: {path}")
            
    except Exception as e:
        logger.error(f"Failed to list bucket contents: {e}", exc_info=True)
        raise

def main():
    while True:
        try:
            logger.info("🚀 Starting Spark ETL job...")
            
            # Create a test SparkSession to verify configuration
            spark = SparkSession.builder \
                .appName("S3ATest") \
                .master("local[*]") \
                .getOrCreate()
            
            # Log all configurations
            log_spark_config(spark)
            
            # Test S3 connection
            test_s3_connection(spark)
            
            # Run the actual ETL job
            spark_clickhouse_run()
            
            logger.info("✅ Spark ETL job completed successfully")
            
            # Wait before next run
            logger.info("⏳ Waiting 60 seconds before next run...")
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"❌ Error in Spark ETL job: {e}", exc_info=True)
            logger.info("⏳ Waiting 60 seconds before retry...")
            time.sleep(60)
        finally:
            if 'spark' in locals():
                spark.stop()

if __name__ == "__main__":
    main() 