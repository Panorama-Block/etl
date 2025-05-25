from confluent_kafka.admin import AdminClient, NewTopic
from .config import KAFKA_BROKER, TOPICS
import logging

logging.info(f"KAFKA_BROKER: {KAFKA_BROKER}")
logging.info(f"TOPICS: {TOPICS}")

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


# Criar o cliente de administração do Kafka
admin_client = AdminClient({
    "bootstrap.servers": KAFKA_BROKER

})

def create_topic():
    """Cria o tópico 'chains_topic' no Kafka"""
    topics = admin_client.list_topics(timeout=10).topics

    if "chains_topic" not in topics:
        new_topic = NewTopic("chains_topic", num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print("🆕 Tópico 'chains_topic' criado com sucesso!")
    else:
        print("🔶 Tópico 'chains_topic' já existe.")

def create_all_topics():
    """Cria todos os tópicos definidos no mapa de tópicos"""
    topics = admin_client.list_topics(timeout=10).topics

    for topic_name in TOPICS:
        if topic_name not in topics:
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            print(f"🆕 Tópico '{topic_name}' criado com sucesso!")
        else:
            print(f"🔶 Tópico '{topic_name}' já existe.")