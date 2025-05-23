# core/config.py
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "default_topic")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "default_group")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")

MINIO_URL = os.getenv("MINIO_URL")
MINIO_USER = os.getenv("MINIO_ACCESS_KEY")
MINIO_PASSWORD = os.getenv("MINIO_SECRET_KEY")
