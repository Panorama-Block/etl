from kafka import KafkaConsumer
import threading
import time
import json
import signal

# Configurações do Kafka
TOPIC_NAME = "chains_topic"
BROKER = "kafka:9092"

# Armazena mensagens consumidas
messages_store = []

# Evento para parar a thread
stop_event = threading.Event()

def consume_kafka():
    """Função que roda em uma thread separada para consumir Kafka"""
    global messages_store

    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='flask-group',
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            print("✅ Conectado ao Kafka! Consumindo mensagens...")
            break
        except Exception as e:
            print(f"Aguardando Kafka e tópico '{TOPIC_NAME}'... Erro: {e}")
            time.sleep(5)

    for message in consumer:
        if stop_event.is_set():
            break  # Sai do loop se o evento de parada for acionado

        messages_store.append(message.value)
        
        if len(messages_store) > 50:  # Mantém apenas as últimas 50 mensagens
            messages_store.pop(0)

    consumer.close()
    print("🛑 Kafka Consumer encerrado.")

# Inicializa a thread do Kafka
consumer_thread = threading.Thread(target=consume_kafka, daemon=True)

def start_kafka():
    """Inicia a thread do Kafka Consumer"""
    consumer_thread.start()

def stop_kafka():
    """Finaliza o Kafka Consumer corretamente"""
    stop_event.set()
    consumer_thread.join()
