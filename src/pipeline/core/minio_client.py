# core/minio_client.py
from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
from io import BytesIO
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('minio.log')
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get MinIO configuration
MINIO_URL = os.getenv("MINIO_URL", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin")

logger.info(f'MinIO Configuration:')
logger.info(f'MINIO_URL: {MINIO_URL}')
logger.info(f'MINIO_USER: {MINIO_USER}')
logger.info(f'MINIO_PASSWORD: {"*" * len(MINIO_PASSWORD) if MINIO_PASSWORD else "None"}')

try:
    client = Minio(
        MINIO_URL.replace("http://", "").replace("https://", ""),  # Remove protocol
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )
    logger.info("MinIO client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize MinIO client: {e}")
    raise

def create_bucket(bucket_name):
    """
    Create a bucket in Minio if it doesn't exist.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        logger.error(f"Error interacting with MinIO: {e}")
        raise

def delete_all_buckets():
    """
    Delete all buckets in MinIO.
    """
    try:
        buckets = client.list_buckets()
        for bucket in buckets:
            try:
                # List all objects in the bucket
                objects = client.list_objects(bucket.name, recursive=True)
                # Delete all objects
                for obj in objects:
                    client.remove_object(bucket.name, obj.object_name)
                # Delete the bucket
                client.remove_bucket(bucket.name)
                logger.info(f"Bucket '{bucket.name}' deleted successfully.")
            except S3Error as e:
                logger.error(f"Error deleting bucket '{bucket.name}': {e}")
    except S3Error as e:
        logger.error(f"Error listing buckets: {e}")
        raise

def upload_data(bucket_name, object_name, data):
    """
    Upload data to MinIO.
    """
    try:
        if not client.bucket_exists(bucket_name):
            create_bucket(bucket_name)
        
        # Convert data to bytes if it's not already
        if not isinstance(data, bytes):
            data = data.encode('utf-8')
        
        # Upload the data
        client.put_object(
            bucket_name,
            object_name,
            BytesIO(data),
            length=len(data)
        )
        logger.info(f"Data uploaded successfully to '{bucket_name}/{object_name}'")
    except S3Error as e:
        logger.error(f"Error uploading data to MinIO: {e}")
        raise

def list_parquet_files(bucket, prefix=""):
    """
    List all .parquet files in the specified bucket (optionally under a prefix).
    """
    parquet_files = []
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            parquet_files.append(obj.object_name)
    return parquet_files