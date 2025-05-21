import threading
import time
import json
import uuid
import logging
import io
import pandas as pd  # Added for DataFrame conversion

from .minio_client import upload_data
from .config import KAFKA_TOPIC, KAFKA_GROUP_ID, KAFKA_BROKER

# Setup logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Armazena mensagens consumidas
messages_store = []

# Evento para parar a thread
stop_event = threading.Event()

def consume_kafka():
    from kafka import KafkaConsumer
    """Função que roda em uma thread separada para consumir Kafka"""
    global messages_store

    while not stop_event.is_set():
        try:
            logger.info(f"🔄 Tentando conectar ao Kafka broker: {KAFKA_BROKER}, tópico: {KAFKA_TOPIC}")

            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            logger.info(f"✅ Conectado ao Kafka! Consumindo mensagens do tópico '{KAFKA_TOPIC}'...")
            break
        except Exception as e:
            logger.error(f"⚠️ Erro ao conectar ao Kafka ({KAFKA_BROKER}): {e}")
            time.sleep(5)  # Aguarda antes de tentar novamente

    for message in consumer:
        if stop_event.is_set():
            break  # Sai do loop se o evento de parada for acionado

        logger.debug(f"📩 Mensagem recebida: {message.value}")

        messages_store.append(message.value)

        # Gerar nome de arquivo único para Minio (usando UUID) com extensão parquet
        file_name = f"data_{uuid.uuid4().hex}.parquet"

        # Converte a mensagem para DataFrame e salva como Parquet
        try:
            # Cria um DataFrame com uma única linha contendo os dados da mensagem
            df = pd.DataFrame([message.value])
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            # Agora, fazemos o upload da mensagem convertida para Parquet
            upload_data('data', file_name, buffer.getvalue())
            logger.info(f"📤 Mensagem salva no MinIO como Parquet: {file_name}")
        except Exception as e:
            logger.error(f"❌ Erro ao enviar mensagem para MinIO: {e}", exc_info=True)

        if len(messages_store) > 50:  # Mantém apenas as últimas 50 mensagens
            messages_store.pop(0)

    consumer.close()
    logger.info("🛑 Kafka Consumer encerrado.")

# Inicializa a thread do Kafka
consumer_thread = threading.Thread(target=consume_kafka, daemon=True)

def start_kafka():
    """Inicia a thread do Kafka Consumer"""
    logger.info("🚀 Iniciando Kafka Consumer...")
    consumer_thread.start()

def stop_kafka():
    """Finaliza o Kafka Consumer corretamente"""
    logger.info("🛑 Parando Kafka Consumer...")
    stop_event.set()
    consumer_thread.join()
    logger.info("✅ Kafka Consumer parado com sucesso.")
