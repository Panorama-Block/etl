#!/usr/bin/env python3
import os
import time
import logging
from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
import subprocess

# Assumes you have a custom module for listing files from MinIO
from .minio_client import list_parquet_files
from .test_spark import run_spark_job


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

def delete_chains_table():
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
    logging.info("Chains table deleted.")

def process_new_file(file_name: str, bucket: str, minio_url: str, minio_user: str, minio_password: str):
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
    clickhouse_table = "chains"
    clickhouse_user = "default"
    clickhouse_password = ""
    spark_version, columns = run_spark_job(
        file_name,
        bucket,
        minio_url,
        minio_user,
        minio_password, 
        clickhouse_url,
        clickhouse_table,
        clickhouse_user,
        clickhouse_password
    )
    if not columns:
        logging.error(f"Error processing file {file_name}: No columns found.")
        return spark_version, []
    return spark_version, columns


def spark_clickhouse_run():
    # Environment/configuration variables.
    bucket = "data"
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
    
    while True:
        logging.info("Listing Parquet files from MinIO...")
        all_files = list_parquet_files(bucket, prefix)
        logging.info(f"Found {len(all_files)} .parquet files in MinIO.")

        loaded_files = get_loaded_files(clickhouse_client)
        logging.info(f"Already loaded files: {loaded_files}")

        # Determine the new files that haven't been processed.
        new_files = [f for f in all_files if f not in loaded_files]
        logging.info(f"New files to process: {new_files}")

        # Process each new file.
        for file_name in new_files:
            logging.info(f"Processing new file: {file_name}")
            spark_version, columns = process_new_file(file_name, bucket, minio_url, minio_user, minio_password)
            logging.info(f"Spark version: {spark_version}")
            logging.info(f"Columns: {columns}")
            # Mark the file as loaded.
            clickhouse_client.execute("INSERT INTO loaded_files (file_name) VALUES", [(file_name,)])

        logging.info("Sleeping for 30 seconds before next check...")
        time.sleep(30)

if __name__ == "__main__":
    spark_clickhouse_run()
