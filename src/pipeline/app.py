import logging
import sys
import threading
import atexit

from .core.minio_client import create_bucket, delete_all_buckets
from .core.kafka_consumer import consume_kafka, start_kafka, stop_kafka
from .core.kafka_admin import create_topic, create_all_topics
from .core.client_clickhouse import spark_clickhouse_run, get_chains_table, delete_all_tables
from .core.spark import run_spark_chains
from .core.kafka_multi_consumer import start_all_consumers, stop_all_consumers

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log')
    ]
)

# Create logger for this module
logger = logging.getLogger(__name__)

def stop_kafka_consumer():
    logger.info("🛑 Parando o Kafka Consumer...")
    stop_kafka()

atexit.register(stop_kafka_consumer)
atexit.register(stop_all_consumers)
atexit.register(delete_all_buckets)
atexit.register(delete_all_tables)

def main():
    # Create bucket if not exist
    create_bucket('data')
    
    # Create topics and start Kafka consumers first
    logger.info("🚀 Iniciando Kafka consumers...")
    create_all_topics()
    start_all_consumers()
    
    # Start Spark processing
    logger.info("🚀 Iniciando processamento Spark...")
    spark_clickhouse_run()

if __name__ == '__main__':
    main()