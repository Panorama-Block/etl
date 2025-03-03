from confluent_kafka.admin import AdminClient, NewTopic

# Criar o cliente de administração do Kafka
admin_client = AdminClient({
    "bootstrap.servers": "kafka:9092"
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