# core/minio_client.py
from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

MINIO_URL = os.getenv("MINIO_URL")
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")

print(f'MINIO_URL: {MINIO_URL}')

client = Minio(
    MINIO_URL,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=False
)

def create_bucket(bucket_name):
    """
    Create a bucket in Minio if it doesn't exist.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error interacting with MinIO: {e}")

def delete_all_buckets():
    """
    Delete all buckets in Minio.
    """
    try:
        for bucket in client.list_buckets():
            client.remove_bucket(bucket.name)
            print(f"Bucket '{bucket.name}' deleted successfully.")
    except S3Error as e:
        print(f"Error deleting buckets in MinIO: {e}")

def upload_data(bucket_name, file_name, data):
    """
    Upload data to Minio as an object in the specified bucket.
    """
    try:
        # Create the bucket if it doesn't exist
        create_bucket(bucket_name)

        # Upload the data to Minio (as an object)
        data_bytes = BytesIO(data)  # Convert string data to bytes
        res = client.put_object(bucket_name, file_name, data_bytes, len(data_bytes.getvalue()))
        print(f"Data uploaded successfully with name: {file_name}")
        return res
    except S3Error as e:
        print(f"Error uploading data to MinIO: {e}")    

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