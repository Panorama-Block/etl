from flask import Flask, request, jsonify
import threading
import atexit

import core.minio_client as minio_client
from core.kafka_consumer import consume_kafka, start_kafka, stop_kafka
from core.kafka_admin import create_topic

app = Flask(__name__)

# Create bucket if not exist
minio_client.create_bucket('data')

def stop_kafka_consumer():
    print("🛑 Parando o Kafka Consumer...")
    stop_kafka()

atexit.register(stop_kafka_consumer)

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

if __name__ == '__main__':
  import logging
  logging.basicConfig(level=logging.DEBUG)
  # Start Kafka Consumer in a separate thread 
  with app.app_context():
      print("🚀 Iniciando o Kafka Consumer...")
      create_topic()
      start_kafka()

  app.run(host="0.0.0.0", port=5000)