import requests
import json

from config import MINIO_URL, MINIO_USER, MINIO_PASSWORD, KAFKA_BROKER, KAFKA_TOPIC

MINIO_API_URL = f"{MINIO_URL}/minio/v3/set-config"

CONFIG_DATA = {
    "kafka": {
        "enable": "on",
        "brokers": KAFKA_BROKER,
        "topic": KAFKA_TOPIC,
        "queueDir": "/tmp"
    }
}

def setup_minio_events():
    """Configure MinIO event notifications to send events to Kafka."""
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(
        MINIO_API_URL,
        auth=(MINIO_USER, MINIO_PASSWORD),  # Correct authentication method
        headers=headers,
        data=json.dumps(CONFIG_DATA),
    )

    if response.status_code == 200:
        print("✅ MinIO event notifications configured successfully!")
    else:
        print(f"❌ Error configuring MinIO: {response.text}")

    response = requests.post(MINIO_API_URL, headers=headers, data=json.dumps(data))
    print(response.text)
