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
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error interacting with MinIO: {e}")

def upload_data(bucket_name, file_name, data):
    try:
        # Create the bucket if it doesn't exist
        create_bucket(bucket_name)

        # Upload the data to Minio (as an object)
        data_bytes = BytesIO(data.encode('utf-8'))  # Convert string data to bytes
        res = client.put_object(bucket_name, file_name, data_bytes, len(data_bytes.getvalue()))
        print(f"Data uploaded successfully with name: {file_name}")
        return res
    except S3Error as e:
        print(f"Error uploading data to MinIO: {e}")
