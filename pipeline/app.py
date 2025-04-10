from flask import Flask, request, jsonify
import threading
import atexit
import threading

import core.minio_client as minio_client
from core.kafka_consumer import consume_kafka, start_kafka, stop_kafka
from core.kafka_admin import create_topic
from core.client_clickhouse import spark_clickhouse_run, get_chains_table, delete_chains_table
from core.test_spark import run_spark_job

app = Flask(__name__)

# Create bucket if not exist
minio_client.create_bucket('data')

def stop_kafka_consumer():
    print("🛑 Parando o Kafka Consumer...")
    stop_kafka()

atexit.register(stop_kafka_consumer)

@app.route('/chains', methods=['GET'])
def get_chains():
    chains = get_chains_table()
    return jsonify(chains)

@app.route('/delete-chains', methods=['DELETE'])
def delete_chains():
    delete_chains_table()
    return jsonify({"message": "Chains table deleted successfully."})

@app.route('/spark', methods=['GET'])
def run_spark():
    print("🚀 Executando o Spark Job...")
    spark_version, avg_age = run_spark_job()
    return jsonify({"spark_version": spark_version, "avg_age": avg_age})

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    # Start Kafka Consumer in a separate thread 
    with app.app_context():
        print("🚀 Iniciando o Kafka Consumer...")
        delete_chains_table()
        create_topic()
        start_kafka()

        # run spark_clickhouse in a separate thread
        spark_thread = threading.Thread(target=spark_clickhouse_run)
        spark_thread.start()

    app.run(host="0.0.0.0", port=5000)