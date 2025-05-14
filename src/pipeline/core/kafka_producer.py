from kafka import KafkaProducer
import json
import time

class KafkaProducer:
    """
    Cliente simples para produzir mensagens de teste (mock) em um tópico Kafka.
    """

    def __init__(self, bootstrap_servers: list[str], topic: str):
        """
        :param bootstrap_servers: lista de brokers, ex: ['localhost:9092']
        :param topic: nome do tópico onde as mensagens serão enviadas
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else None,
            linger_ms=10,      # opcional: agrupa mensagens por até 10 ms
            acks='all'         # aguarda confirmação de todos os brokers para garantir persistência
        )

    def send_mock(self, data: dict, key: str | None = None) -> None:
        """
        Envia um dicionário mockado como JSON ao tópico configurado.
        :param data: payload (dict) que será serializado em JSON
        :param key: opcional, chave da mensagem (pode ser usada para particionamento)
        """
        future = self.producer.send(self.topic, value=data, key=key)
        # opcional: aguarda confirmação síncrona (útil em testes)
        record_metadata = future.get(timeout=10)
        print(f"✔ Mensagem enviada para {record_metadata.topic} "
              f"[partition {record_metadata.partition} @ offset {record_metadata.offset}]")

    def close(self) -> None:
        """Flusha e fecha o producer."""
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    # Configuração do producer
    bootstrap = ['localhost:29092']
    topic = 'topic-test'

    producer = KafkaProducer(bootstrap, topic)

    # Dados mockados 
    mock_payload = {
        "id": "1",
        "timestamp": "2023-01-01 12:00:00",
        "value": 100.0,
        "status": "ok"
    }

    producer.send_mock(mock_payload)

    producer.close()