from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_URL = os.getenv("MINIO_URL")
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")

client = Minio(
        MINIO_URL, 
        access_key=MINIO_USER, 
        secret_key=MINIO_PASSWORD,
        secure=False
    )

def create_bucket(bucket_name): 
  try:
        # create bucket if not exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created with success")
        else:
            print(f"Bucket '{bucket_name}' exists.")
  except S3Error as e:
        print(f"Error to interact with MinIO: {e}")

def upload_file(bucket_name, filepath, filename):
  try:
      file_name = os.path.basename(filepath)
      res = client.fput_object(bucket_name, file_name, filepath)
      print("Success to upload")
      return res
  
  except S3Error as e:
        print(f"Erro to interact with MinIO: {e}")

def download_file(bucket_name, filename, local_path):
  try: 
    res = client.fget_object(bucket_name, filename, local_path)
    return  f"File downloaded successfully to {local_path}"
  
  except S3Error as e:
    print(f"Error to interact with MinIO: {e}")