import os
import time
import json
from io import BytesIO

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from clickhouse_driver import Client as ClickHouseClient
import pyarrow.parquet as pq
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# MinIO Configuration
MINIO_URL = 'localhost:9000'  # Changed from minio:9000 to localhost:9000
MINIO_ACCESS_KEY = os.getenv('MINIO_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_PASSWORD', 'minioadmin')
MINIO_BUCKET_NAME = 'chains'
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
MAX_RETRIES = 12
RETRY_INTERVAL = 10

def verify_minio_data(minio_client):
    """
    Verifies if the expected data is present in any Parquet file in the MinIO bucket.
    """
    print(f"Verifying MinIO bucket '{MINIO_BUCKET_NAME}'...")
    try:
        # First check if bucket exists
        if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
            print(f"Bucket '{MINIO_BUCKET_NAME}' does not exist yet.")
            return False

        objects = minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True)
        parquet_files_found = False
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                parquet_files_found = True
                # print(f"Found Parquet file: {obj.object_name}. Checking content...")
                # progress bar
                try:
                    response = minio_client.get_object(MINIO_BUCKET_NAME, obj.object_name)
                    file_content = BytesIO(response.read())
                    
                    # Read Parquet file into a Pandas DataFrame
                    table = pq.read_table(file_content)
                    df = table.to_pandas()

                    if df.empty:
                        print(f"Parquet file {obj.object_name} is empty.")
                        continue

                    # Print column names for debugging
                    # print(f"Columns in Parquet file: {df.columns.tolist()}")

                    # Check if 'chain' column exists
                    if 'chain' not in df.columns:
                        # print(f"'chain' column not found in {obj.object_name}. Available columns: {df.columns.tolist()}")
                        continue

                    # Convert chain column to dictionary if it's a string
                    if df['chain'].dtype == 'object':
                        try:
                            # Try to parse JSON strings in the chain column
                            df['chain'] = df['chain'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                        except Exception as e:
                            print(f"Error parsing chain column: {str(e)}")
                            continue

                    # Extract chainId and chainName from the chain column
                    try:
                        df['chainId'] = df['chain'].apply(lambda x: x.get('chainId') if isinstance(x, dict) else None)
                        df['chainName'] = df['chain'].apply(lambda x: x.get('chainName') if isinstance(x, dict) else None)
                        
                        # Print sample of extracted data
                        # print("Sample of extracted data:")
                        # print(df[['chainId', 'chainName']].head())
                        
                        # Check if the expected data exists in the DataFrame
                        match = df[
                            (df['chainId'] == EXPECTED_CHAIN_ID) &
                            (df['chainName'] == EXPECTED_CHAIN_NAME)
                        ]
                        if not match.empty:
                            print(f"SUCCESS: Expected data found in MinIO object '{obj.object_name}'.")
                            return True
                        else:
                            pass
                            # print(f"Data not found in {obj.object_name}. First few rows of extracted data:")
                            # print(df[['chainId', 'chainName']].head())
                    except Exception as e:
                        print(f"Error extracting data from chain column: {str(e)}")
                        print("Sample of chain column:")
                        print(df['chain'].head())
                except Exception as e:
                    print(f"Error reading or processing Parquet file {obj.object_name}: {str(e)}")
                    print(f"Error type: {type(e).__name__}")
                finally:
                    response.close()
                    response.release_conn()
        
        if not parquet_files_found:
            print("No Parquet files found in the bucket yet.")
        return False
    except S3Error as e:
        print(f"MinIO error: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during MinIO verification: {e}")
        return False

def verify_clickhouse_data(ch_client):
    """
    Verifies if the expected data is present in the ClickHouse table.
    """
    print(f"Verifying ClickHouse table '{CLICKHOUSE_TABLE_NAME}'...")
    try:
        # First check if table exists
        tables = ch_client.execute('SHOW TABLES')
        if not any(table[0] == CLICKHOUSE_TABLE_NAME for table in tables):
            print(f"Table '{CLICKHOUSE_TABLE_NAME}' does not exist yet.")
            return False

        query = f"""
        SELECT chainId, chainName
        FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE_NAME}
        WHERE chainId = '{EXPECTED_CHAIN_ID}' AND chainName = '{EXPECTED_CHAIN_NAME}'
        LIMIT 1
        """
        result = ch_client.execute(query)
        if result:
            print(f"SUCCESS: Expected data found in ClickHouse table '{CLICKHOUSE_TABLE_NAME}'. Row: {result[0]}")
            return True
        else:
            print("Data not found in ClickHouse yet.")
            return False
    except Exception as e:
        print(f"ClickHouse error: {e}")
        return False

if __name__ == "__main__":
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("ERROR: MINIO_USER and/or MINIO_PASSWORD environment variables not set.")
        print("Please ensure they are defined in your .env file in the project root.")
        exit(1)

    print("Starting E2E data verification...")
    print(f"Will check MinIO bucket '{MINIO_BUCKET_NAME}' and ClickHouse table '{CLICKHOUSE_TABLE_NAME}'.")
    print(f"Looking for chainId='{EXPECTED_CHAIN_ID}', chainName='{EXPECTED_CHAIN_NAME}'.")

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
        print(f"\nAttempt {i + 1}/{MAX_RETRIES}...")

        if not minio_verified:
            minio_verified = verify_minio_data(minio_client)
        
        if not clickhouse_verified:
            clickhouse_verified = verify_clickhouse_data(ch_client)

        if minio_verified and clickhouse_verified:
            print("\n-------------------------------------------")
            print("E2E Test SUCCESSFUL: Data verified in MinIO and ClickHouse!")
            print("-------------------------------------------")
            exit(0)
        
        if i < MAX_RETRIES - 1:
            print(f"Waiting {RETRY_INTERVAL} seconds before next check...")
            time.sleep(RETRY_INTERVAL)

    print("\n-------------------------------------------")
    print("E2E Test FAILED: Data not found in one or both systems after timeout.")
    if not minio_verified:
        print("- Data NOT VERIFIED in MinIO.")
    if not clickhouse_verified:
        print("- Data NOT VERIFIED in ClickHouse.")
    print("-------------------------------------------")
    exit(1) 