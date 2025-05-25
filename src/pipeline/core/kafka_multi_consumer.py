import threading
import time
import json
import uuid
import logging
import io
import pandas as pd
import datetime

from .minio_client import upload_data, create_bucket
from .config import KAFKA_BROKER, KAFKA_GROUP_ID, TOPICS

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
            logger.error(f"❌ Erro ao conectar no tópico {topic_name}: {e}", exc_info=True)
            time.sleep(5)

    logger.info(f"🎯 Iniciando consumo de mensagens do tópico '{topic_name}'")
    message_count = 0

    for message in consumer:
        if stop_event.is_set():
            break

        message_count += 1
        logger.info(f"[{topic_name}] 📩 Mensagem #{message_count} recebida")

        try:
            data = message.value
            logger.info(f"[{topic_name}] Tipo da mensagem: {type(data)}")
            logger.info(f"[{topic_name}] Conteúdo da mensagem: {data}")

            if isinstance(data, str):
                logger.info(f"[{topic_name}] Mensagem recebida como string, convertendo para JSON.")
                data = json.loads(message.value)
            elif isinstance(data, dict):
                logger.info(f"[{topic_name}] Mensagem recebida como dicionário.")
            else:
                logger.warning(f"[{topic_name}] Tipo de mensagem inesperado: {type(data)}")

            if topic_name == "avax_metrics":
                data = data.get("block")
                logger.info(f"[{topic_name}] Bloco: {data}")
                
                
            df = pd.DataFrame([data])  # Wrap data in a list to handle scalar values
            logger.info(f"[{topic_name}] DataFrame criado com colunas: {df.columns.tolist()}")
            
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)  

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{topic_name}_{timestamp}_{uuid.uuid4().hex}.parquet"
            
            logger.info(f"[{topic_name}] 📤 Tentando fazer upload para MinIO: bucket='{bucket_name}', arquivo='{file_name}'")
            upload_data(bucket_name, file_name, buffer.getvalue())
            logger.info(f"[{topic_name}] ✅ Upload concluído com sucesso!")

        except Exception as e:
            logger.error(f"[{topic_name}] ❌ Erro ao processar mensagem: {e}", exc_info=True)

    logger.info(f"🛑 Consumer encerrado para o tópico '{topic_name}'. Total de mensagens processadas: {message_count}")

def start_all_consumers():
    """Start consumers for all topics"""
    logger.info("🚀 Iniciando todos os consumers...")
    
    # Create buckets for each topic
    for topic in TOPICS:
        try:
            create_bucket(topic)
            logger.info(f"✅ Bucket criado/verificado para o tópico: {topic}")
        except Exception as e:
            logger.error(f"❌ Erro ao criar bucket para tópico {topic}: {e}", exc_info=True)
    
    # Start consumer for each topic
    for topic in TOPICS:
        try:
            stop_event = threading.Event()
            stop_events[topic] = stop_event
            
            thread = threading.Thread(
                target=consume_topic,
                args=(topic, topic, stop_event),
                daemon=True
            )
            consumer_threads[topic] = thread
            thread.start()
            logger.info(f"✅ Consumer iniciado para o tópico: {topic}")
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar consumer para tópico {topic}: {e}", exc_info=True)
    
    logger.info("✨ Todos os consumers foram iniciados!")

def stop_all_consumers():
    """Stop all consumer threads"""
    for topic, stop_event in stop_events.items():
        stop_event.set()
        logger.info(f"🛑 Stopping consumer for topic: {topic}")
    
    for topic, thread in consumer_threads.items():
        thread.join()
        logger.info(f"✅ Consumer stopped for topic: {topic}")