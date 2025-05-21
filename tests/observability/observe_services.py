import socket
import requests
from kafka import KafkaAdminClient
from minio import Minio
from clickhouse_connect import get_client
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configurations
KAFKA_BROKER = 'localhost:29092'
MINIO_URL = 'localhost:9000'
MINIO_USER = os.getenv('MINIO_USER', 'minioadmin')
MINIO_PASSWORD = os.getenv('MINIO_PASSWORD', 'minioadmin')
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
FLASK_URL = 'http://localhost:5000'

def check_port(host, port, name):
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"[OK] {name} is reachable at {host}:{port}")
            return True
    except Exception as e:
        print(f"[FAIL] {name} not reachable at {host}:{port} ({e})")
        return False

def check_flask():
    try:
        r = requests.get(FLASK_URL + '/health')
        print(f"[OK] Flask app running: {r.status_code} {r.reason}")
        return True
    except Exception as e:
        print(f"[FAIL] Flask app not reachable: {e}")
        return False

def check_kafka():
    try:
        print(f"\nChecking Kafka at {KAFKA_BROKER}...")
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        print(f"[OK] Kafka topics: {topics}")
        return True
    except Exception as e:
        print(f"[FAIL] Kafka error: {e}")
        return False

def check_minio():
    try:
        print(f"\nChecking MinIO at {MINIO_URL}...")
        client = Minio(
            MINIO_URL,
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=False
        )
        buckets = client.list_buckets()
        print(f"[OK] MinIO buckets: {[b.name for b in buckets]}")
        return True
    except Exception as e:
        print(f"[FAIL] MinIO error: {e}")
        return False

def check_clickhouse():
    try:
        print(f"\nChecking ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
        client = get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            connect_timeout=5,
            send_receive_timeout=5
        )
        tables = client.query('SHOW TABLES').result_rows
        print(f"[OK] ClickHouse tables: {tables}")
        return True
    except Exception as e:
        print(f"[FAIL] ClickHouse error: {e}")
        return False

def main():
    print("=== Observability Check ===")
    print("\nChecking service ports...")
    kafka_ok = check_port('localhost', 29092, 'Kafka')
    minio_ok = check_port('localhost', 9000, 'MinIO')
    clickhouse_ok = check_port('localhost', 8123, 'ClickHouse')
    flask_ok = check_port('localhost', 5000, 'Flask app')
    
    print("\nChecking service details...")
    if kafka_ok:
        check_kafka()
    if minio_ok:
        check_minio()
    if clickhouse_ok:
        check_clickhouse()
    if flask_ok:
        check_flask()

if __name__ == "__main__":
    main()