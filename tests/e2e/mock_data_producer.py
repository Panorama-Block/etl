import json
from kafka import KafkaProducer
import time
import sys

# Kafka Configuration
KAFKA_BROKER_URL = 'localhost:29092'  # Assumes Kafka is accessible from the host
TOPIC_NAME = 'chains_topic'

# Mock data to be sent
mock_data = {
    "type": "chain.updated",
    "chain": {
        "chainId": "test-chain-001",
        "chainName": "TestChain Alpha",
        "status": "OK",
        "description": "A test chain for E2E validation",
        "platformChainId": "platform-x",
        "subnetId": "subnet-alpha",
        "vmId": "vm-evm-01",
        "vmName": "EVM",
        "explorerUrl": "http://localhost/explorer/test-chain-001",
        "rpcUrl": "http://localhost/rpc/test-chain-001",
        "wsUrl": "ws://localhost/ws/test-chain-001",
        "isTestnet": True,
        "utilityAddresses": {
            "multicall": "0x1234567890123456789012345678901234567890"
        },
        "networkToken": {
            "name": "Wrapped AVAX",
            "symbol": "WAVAX",
            "decimals": 18,
            "logoUri": "https://images.ctfassets.net/gcj8jwzm6086/5VHupNKwnDYJvqMENeV7iJ/fdd6326b7a82c8388e4ee9d4be7062d4/avalanche-avax-logo.svg",
            "description": "The native token of the Avalanche platform, wrapped."
        },
        "chainLogoUri": "http://example.com/logo.png",
        "private": True,
        "enabledFeatures": [
            "nftIndexing"
        ]
    }
}

def send_message_to_kafka(producer, topic, data):
    try:
        # Serialize data to JSON and then encode to bytes
        message_bytes = json.dumps(data).encode('utf-8')
        # Send the message
        future = producer.send(topic, message_bytes)
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        print(f"Message sent to topic '{topic}' at offset {record_metadata.offset}")
        return True
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")
        return False

def create_producer(max_retries=5, retry_delay=5):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} to connect to Kafka broker at {KAFKA_BROKER_URL}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                acks='all',
                retries=5,
                request_timeout_ms=30000
            )
            print("Successfully connected to Kafka broker.")
            return producer
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Could not connect to Kafka.")
                return None

if __name__ == "__main__":
    producer = None
    try:
        producer = create_producer()
        if producer is None:
            print("Failed to create Kafka producer. Exiting.")
            sys.exit(1)
        
        print(f"Sending mock data to topic '{TOPIC_NAME}'...")
        if send_message_to_kafka(producer, TOPIC_NAME, mock_data):
            print("Mock data sent successfully.")
        else:
            print("Failed to send mock data.")
            sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if producer:
            print("Closing Kafka producer.")
            producer.flush()
            producer.close()
            print("Kafka producer closed.") 