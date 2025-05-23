import os
import time
import json
from io import BytesIO
import logging
from datetime import datetime

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from clickhouse_driver import Client as ClickHouseClient
import pyarrow.parquet as pq
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Setup logging
LOG_FILE = 'e2e_verification.log'

def setup_logging():
    """Setup logging configuration"""
    # Clean the log file before each run
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()  # Keep console output for immediate feedback
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# --- Configuration ---
# MinIO Configuration
MINIO_URL = 'localhost:9000'  # Changed from minio:9000 to localhost:9000
MINIO_ACCESS_KEY = os.getenv('MINIO_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_PASSWORD', 'minioadmin')
MINIO_BUCKETS = ['data', 'chains']  # Check both buckets
MINIO_SECURE = False

# ClickHouse Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000  # Changed from 8123 to 9000 for native protocol
CLICKHOUSE_DATABASE = 'default'
CLICKHOUSE_TABLE_NAME = 'chains'

# Expected data from the mock producer
EXPECTED_CHAIN_ID = "test-chain-001"
EXPECTED_CHAIN_NAME = "TestChain Alpha"

# Verification retry settings
MAX_RETRIES = 2
RETRY_INTERVAL = 10

def verify_minio_data(minio_client):
    """
    Verifies if the expected data is present in any Parquet file in the MinIO buckets.
    """
    logger.info("\n=== MinIO Verification Details ===")
    
    for bucket_name in MINIO_BUCKETS:
        logger.info(f"\nChecking bucket '{bucket_name}'...")
        try:
            # First check if bucket exists
            if not minio_client.bucket_exists(bucket_name):
                logger.error(f"❌ Bucket '{bucket_name}' does not exist yet.")
                continue
            logger.info(f"✅ Bucket '{bucket_name}' exists.")

            objects = list(minio_client.list_objects(bucket_name, recursive=True))
            logger.info(f"Found {len(objects)} objects in bucket '{bucket_name}'.")
            
            parquet_files_found = False
            for obj in objects:
                logger.info(f"\nChecking object: {obj.object_name}")
                if obj.object_name.endswith('.parquet'):
                    parquet_files_found = True
                    logger.info(f"Found Parquet file: {obj.object_name}")
                    try:
                        response = minio_client.get_object(bucket_name, obj.object_name)
                        file_content = BytesIO(response.read())
                        
                        # Read Parquet file into a Pandas DataFrame
                        table = pq.read_table(file_content)
                        df = table.to_pandas()

                        if df.empty:
                            logger.error(f"❌ Parquet file {obj.object_name} is empty.")
                            continue

                        logger.info(f"✅ Parquet file contains {len(df)} rows")
                        logger.info(f"Columns found: {df.columns.tolist()}")
                        logger.info("\nFirst row of data:")
                        logger.info(df.iloc[0].to_dict())

                        # Check if 'chain' column exists
                        if 'chain' not in df.columns:
                            logger.error(f"❌ 'chain' column not found. Available columns: {df.columns.tolist()}")
                            continue

                        logger.info("\nSample of chain column data:")
                        logger.info(df['chain'].head())

                        # Convert chain column to dictionary if it's a string
                        if df['chain'].dtype == 'object':
                            try:
                                # Try to parse JSON strings in the chain column
                                df['chain'] = df['chain'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                                logger.info("Successfully parsed chain column JSON data")
                            except Exception as e:
                                logger.error(f"❌ Error parsing chain column: {str(e)}")
                                continue

                        # Extract chainId and chainName from the chain column
                        try:
                            df['chainId'] = df['chain'].apply(lambda x: x.get('chainId') if isinstance(x, dict) else None)
                            df['chainName'] = df['chain'].apply(lambda x: x.get('chainName') if isinstance(x, dict) else None)
                            
                            logger.info("\nSample of extracted data:")
                            logger.info(df[['chainId', 'chainName']].head())
                            
                            # Check if the expected data exists in the DataFrame
                            match = df[
                                (df['chainId'] == EXPECTED_CHAIN_ID) &
                                (df['chainName'] == EXPECTED_CHAIN_NAME)
                            ]
                            if not match.empty:
                                logger.info(f"\n✅ SUCCESS: Found {len(match)} matching records in MinIO object '{obj.object_name}'")
                                return True
                            else:
                                logger.error(f"\n❌ Expected data not found in {obj.object_name}")
                                logger.error(f"Looking for chainId='{EXPECTED_CHAIN_ID}', chainName='{EXPECTED_CHAIN_NAME}'")
                        except Exception as e:
                            logger.error(f"❌ Error extracting data from chain column: {str(e)}")
                            logger.error("Sample of chain column:")
                            logger.error(df['chain'].head())
                    except Exception as e:
                        logger.error(f"❌ Error reading or processing Parquet file {obj.object_name}: {str(e)}")
                        logger.error(f"Error type: {type(e).__name__}")
                    finally:
                        response.close()
                        response.release_conn()
            
            if not parquet_files_found:
                logger.error(f"❌ No Parquet files found in bucket '{bucket_name}' yet.")
        except S3Error as e:
            logger.error(f"❌ MinIO error for bucket '{bucket_name}': {e}")
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred during MinIO verification for bucket '{bucket_name}': {e}")
    
    return False

def verify_clickhouse_data(ch_client):
    """
    Verifies if the expected data is present in the ClickHouse table.
    """
    logger.info(f"\nVerifying ClickHouse table '{CLICKHOUSE_TABLE_NAME}'...")
    try:
        # First check if table exists
        tables = ch_client.execute('SHOW TABLES')
        if not any(table[0] == CLICKHOUSE_TABLE_NAME for table in tables):
            logger.error(f"❌ Table '{CLICKHOUSE_TABLE_NAME}' does not exist yet.")
            return False

        query = f"""
        SELECT chainId, chainName
        FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE_NAME}
        WHERE chainId = '{EXPECTED_CHAIN_ID}' AND chainName = '{EXPECTED_CHAIN_NAME}'
        LIMIT 1
        """
        result = ch_client.execute(query)
        if result:
            logger.info(f"✅ SUCCESS: Expected data found in ClickHouse table '{CLICKHOUSE_TABLE_NAME}'. Row: {result[0]}")
            return True
        else:
            logger.error("❌ Data not found in ClickHouse yet.")
            return False
    except Exception as e:
        logger.error(f"❌ ClickHouse error: {e}")
        return False

if __name__ == "__main__":
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        logger.error("ERROR: MINIO_USER and/or MINIO_PASSWORD environment variables not set.")
        logger.error("Please ensure they are defined in your .env file in the project root.")
        exit(1)

    logger.info("Starting E2E data verification...")
    logger.info(f"Will check MinIO buckets '{', '.join(MINIO_BUCKETS)}' and ClickHouse table '{CLICKHOUSE_TABLE_NAME}'.")
    logger.info(f"Looking for chainId='{EXPECTED_CHAIN_ID}', chainName='{EXPECTED_CHAIN_NAME}'.")
    logger.info(f"Log file: {os.path.abspath(LOG_FILE)}")

    minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    ch_client = ClickHouseClient(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT
    )
    
    minio_verified = False
    clickhouse_verified = False

    for i in range(MAX_RETRIES):
        logger.info(f"\nAttempt {i + 1}/{MAX_RETRIES}...")

        if not minio_verified:
            minio_verified = verify_minio_data(minio_client)
        
        if not clickhouse_verified:
            clickhouse_verified = verify_clickhouse_data(ch_client)

        if minio_verified and clickhouse_verified:
            logger.info("\n-------------------------------------------")
            logger.info("E2E Test SUCCESSFUL: Data verified in MinIO and ClickHouse!")
            logger.info("-------------------------------------------")
            exit(0)
        
        if i < MAX_RETRIES - 1:
            logger.info(f"Waiting {RETRY_INTERVAL} seconds before next check...")
            time.sleep(RETRY_INTERVAL)

    logger.error("\n-------------------------------------------")
    logger.error("E2E Test FAILED: Data not found in one or both systems after timeout.")
    if not minio_verified:
        logger.error("- Data NOT VERIFIED in MinIO.")
    if not clickhouse_verified:
        logger.error("- Data NOT VERIFIED in ClickHouse.")
    logger.error("-------------------------------------------")
    exit(1) 