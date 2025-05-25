#!/usr/bin/env python3
import os
import time
import logging
from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
import subprocess
from .config import TOPICS
# Assumes you have a custom module for listing files from MinIO
from .minio_client import list_parquet_files
from .spark import SparkArgs
from .spark import (
    run_spark_chains, 
    run_spark_blocks, 
    run_spark_transactions, 
    run_spark_logs, 
    run_spark_erc20, 
    run_spark_erc721, 
    run_spark_erc1155, 
    run_spark_internal_tx, 
    run_spark_metrics, 
    run_spark_metrics_activity, 
    run_spark_metrics_performance, 
    run_spark_metrics_gas, 
    run_spark_metrics_cumulative, 
    run_spark_subnets, 
    run_spark_blockchains, 
    run_spark_validators, 
    run_spark_delegators, 
    run_spark_bridges
)

logging.basicConfig(level=logging.INFO)

'''
Converts the topic name to the spark function name.
'''
spark_functions = {
    "avalanche.chains": run_spark_chains,
    "avalanche.blocks.43114": run_spark_blocks,
    "avalanche.transactions.43114": run_spark_transactions,
    "avalanche.logs.43114": run_spark_logs,
    "avalanche.erc20.43114": run_spark_erc20,
    "avalanche.erc721.43114": run_spark_erc721,
    "avalanche.erc1155.43114": run_spark_erc1155,
    "avalanche.internal-tx.43114": run_spark_internal_tx,
    "avax.metrics": run_spark_metrics,
    "avax.metrics.activity": run_spark_metrics_activity,
    "avax.metrics.performance": run_spark_metrics_performance,
    "avax.metrics.gas": run_spark_metrics_gas,
    "avax.metrics.cumulative": run_spark_metrics_cumulative,
    "avalanche.subnets": run_spark_subnets,
    "avalanche.blockchains": run_spark_blockchains,
    "avalanche.validators": run_spark_validators,
    "avalanche.delegators": run_spark_delegators,
    "avalanche.bridges": run_spark_bridges,
}

logging.basicConfig(level=logging.INFO)

def create_tables(clickhouse_client: Client, sql_path: str):
    """
    Create tables in ClickHouse by executing the SQL statements from the given file.
    """
    logging.info(f"Creating tables from {sql_path}")
    base_path = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(base_path, sql_path)
    with open(full_path, 'r') as f:
        sql = f.read()
    for stmt in (s.strip() for s in sql.split(';')):
        if stmt:
            logging.info(f"Executing statement: {stmt}")
            clickhouse_client.execute(stmt)

def get_loaded_files(clickhouse_client: Client):
    """
    Retrieve the set of already processed file names from the ClickHouse loaded_files table.
    """
    logging.info("Retrieving list of already loaded files...")
    result = clickhouse_client.execute("SELECT file_name FROM loaded_files")
    return {row[0] for row in result}

def get_chains_table():
    """
    Retrieve the chains table from ClickHouse.
    """
    # Connect to ClickHouse.
    clickhouse_client = Client(
        host="clickhouse",
        port=9000,
        user="default",
        password=""
    )
    
    logging.info("Retrieving chains table...")
    result = clickhouse_client.execute("SELECT * FROM chains")
    return result

def delete_all_tables():
    """
    Delete the chains table from ClickHouse.
    """
    # Connect to ClickHouse.
    clickhouse_client = Client(
        host="clickhouse",
        port=9000,
        user="default",
        password=""
    )
    
    logging.info("Deleting chains table...")
    clickhouse_client.execute("DROP TABLE IF EXISTS chains")
    clickhouse_client.execute("DROP TABLE IF EXISTS loaded_files")
    clickhouse_client.execute("DROP TABLE IF EXISTS blocks")
    logging.info("Chains table deleted.")

def process_new_file(file_name: str, bucket: str, minio_url: str, minio_user: str, minio_password: str):
    """
    Process a new file from MinIO and load it into ClickHouse.
    
    Args:
        file_name: Name of the file to process
        bucket: Topic/bucket name
        minio_url: MinIO server URL
        minio_user: MinIO username
        minio_password: MinIO password
    """
    logging.info(f"Processing file: {file_name} from client_clickhouse.py")
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
    clickhouse_user = "default"
    clickhouse_password = ""

    spark_args = SparkArgs(
        file_name=file_name,
        bucket=bucket,
        minio_url=minio_url,
        minio_user=minio_user,
        minio_password=minio_password,
        clickhouse_url=clickhouse_url,
        clickhouse_user=clickhouse_user,
        clickhouse_password=clickhouse_password
    )

    function = spark_functions[bucket]
    spark_version, columns = function(spark_args)

    if not columns:
        logging.error(f"Error processing file {file_name}: No columns found.")
        return spark_version, []
    return spark_version, columns


def spark_clickhouse_run():
    prefix = ""  # Adjust if needed.
    minio_url = "http://minio:9000"  # MinIO endpoint.
    minio_user = os.environ.get("MINIO_USER", "minio_user")
    minio_password = os.environ.get("MINIO_PASSWORD", "minio_password")

    # Connect to ClickHouse.
    clickhouse_client = Client(
        host="clickhouse",
        port=9000,
        user="default",
        password=""
    )

    # Create tables if they don't already exist.
    create_tables(clickhouse_client, "../sql/create_tables.sql")
    
    logging.info("✨ Starting Spark-ClickHouse processing...")

    include_topics = [
        "avalanche.transactions.43114"
        ]
    
    while True:
        logging.info("listing parquet files from minio...")

        # Get already loaded files once
        loaded_files = get_loaded_files(clickhouse_client)
        # logging.info(f"already loaded files: {loaded_files}")

        # Process each topic
        for topic in TOPICS:
            if topic not in include_topics:
                continue
            logging.info(f"Processing topic: {topic}")
            topic_files = list_parquet_files(topic, prefix)
            logging.info(f"found {len(topic_files)} files for topic {topic}")

            # determine the new files that haven't been processed
            new_files = [f for f in topic_files if f not in loaded_files]
            logging.info(f"new files to process for topic {topic}: {new_files}")

            # process each new file
            for file_name in new_files:
                logging.info(f"🖿 processing new file: {file_name}")
                spark_version, columns = process_new_file(file_name, topic, minio_url, minio_user, minio_password)
                logging.info(f"🖿 spark version: {spark_version}")
                logging.info(f"🖿 columns: {columns}")
                # mark the file as loaded
                clickhouse_client.execute("insert into loaded_files (file_name) values", [(file_name,)])

        logging.info("sleeping for 30 seconds before next check...")
        time.sleep(30)

if __name__ == "__main__":
    spark_clickhouse_run()
