import threading
import time
import json
import uuid
import logging
import io
import pandas as pd

from .minio_client import upload_data
from .config import KAFKA_BROKER, KAFKA_GROUP_ID

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Map de tópicos para buckets
TOPICS = {
    "chains_topic": "chains",
    "blocks_topic": "blocks",
    "transactions_topic": "transactions",
    "logs_topic": "logs",
    "erc20_topic": "erc20",
    "erc721_topic": "erc721",
    "erc1155_topic": "erc1155",
    "metrics_topic": "metrics",
    "metrics_activity_topic": "metrics_activity",
    "metrics_performance_topic": "metrics_performance",
    "metrics_gas_topic": "metrics_gas",
    "metrics_cumulative_topic": "metrics_cumulative",
    "subnets_topic": "subnets",
    "blockchains_topic": "blockchains",
    "validators_topic": "validators",
    "delegators_topic": "delegators",
    "bridges_topic": "bridges"
}

# TOPICS = {
#     "9cT3GzNxcLWFXGAgqdJsydZkh9ajKEXn4hKvkRLJHgwv.tokens":        "avax_tokens",
#     "9cT3GzNxcLWFXGAgqdJsydZkh9ajKEXn4hKvkRLJHgwv.transactions": "avax_transactions",
#     "9cT3GzNxcLWFXGAgqdJsydZkh9ajKEXn4hKvkRLJHgwv.factories":    "avax_factories",
#     "9cT3GzNxcLWFXGAgqdJsydZkh9ajKEXn4hKvkRLJHgwv.swaps":        "avax_swaps",

#     "9EAxYE17Cc478uzFXRbM7PVnMUSsgb99XZiGxodbtpbk.tokens":       "eth_tokens",
#     "9EAxYE17Cc478uzFXRbM7PVnMUSsgb99XZiGxodbtpbk.transactions":"eth_transactions",
#     "9EAxYE17Cc478uzFXRbM7PVnMUSsgb99XZiGxodbtpbk.factories":    "eth_factories",
#     "9EAxYE17Cc478uzFXRbM7PVnMUSsgb99XZiGxodbtpbk.swaps":        "eth_swaps",
# }

# Armazena todas as threads e stop_events
consumer_threads = {}
stop_events = {}

def consume_topic(topic_name, bucket_name, stop_event):
    """Consome mensagens de um tópico Kafka e envia para o MinIO"""
    from kafka import KafkaConsumer
    while not stop_event.is_set():
        try:
            logger.info(f"🔄 Conectando ao Kafka | Tópico: {topic_name}")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f"{KAFKA_GROUP_ID}-{topic_name}",
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            logger.info(f"✅ Conectado ao tópico '{topic_name}'!")
            break
        except Exception as e:
            logger.error(f"❌ Erro ao conectar no tópico {topic_name}: {e}")
            time.sleep(5)

    for message in consumer:
        if stop_event.is_set():
            break

        logger.debug(f"[{topic_name}] 📩 Mensagem recebida: {message.value}")

        try:
            data = message.value
            if isinstance(data, str):
                logger.debug(f"[{topic_name}] Mensagem recebida como string, convertendo para JSON.")
                data = json.loads(message.value)
            else:
                logger.debug(f"Isntance: {type(data)}")
                
            data_type = data.get("type", "unknown")
            # if data_type constains "chain"
            if "chain" in data_type:
                key = "chain"
            elif "block" in data_type:
                key = "block"
            else:
                logger.warning(f"[{topic_name}] ⚠️ Tipo de dado desconhecido: {data_type}")
                continue
            df = pd.DataFrame([data.get(key, {})])
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)  

            file_name = f"{topic_name}_{uuid.uuid4().hex}.parquet"
            upload_data(bucket_name, file_name, buffer.getvalue())

            logger.info(f"[{topic_name}] 📤 Enviado ao MinIO no bucket '{bucket_name}': {file_name}")
        except Exception as e:
            logger.error(f"[{topic_name}] ❌ Erro ao salvar no MinIO: {e}", exc_info=True)

    consumer.close()
    logger.info(f"🛑 Consumer encerrado para o tópico '{topic_name}'.")

def start_all_consumers():
    """Inicia uma thread de consumo para cada tópico"""
    for topic, bucket in TOPICS.items():
        stop_event = threading.Event()
        thread = threading.Thread(target=consume_topic, args=(topic, bucket, stop_event), daemon=True)

        stop_events[topic] = stop_event
        consumer_threads[topic] = thread

        logger.info(f"🚀 Iniciando consumidor para o tópico '{topic}'")
        thread.start()

def stop_all_consumers():
    """Para todos os consumidores"""
    logger.info("🛑 Encerrando todos os consumidores Kafka...")
    for topic, stop_event in stop_events.items():
        logger.info(f"⏹️ Encerrando tópico '{topic}'...")
        stop_event.set()

    for topic, thread in consumer_threads.items():
        thread.join()
        logger.info(f"✅ Thread finalizada para o tópico '{topic}'")