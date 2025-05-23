import threading
import pytest
from unittest.mock import patch

# Ajuste o import abaixo conforme o caminho real do seu módulo de consumer
import src.pipeline.core.kafka_consumer as consumer_mod

# Classes fake para simular mensagens Kafka e o consumidor
class FakeMessage:
    def __init__(self, value):
        self.value = value

class FakeConsumer:
    def __iter__(self):
        # Duas mensagens de teste
        return iter([FakeMessage({'foo': 1}), FakeMessage({'bar': 2})])
    def close(self):
        pass

class EmptyConsumer:
    def __iter__(self):
        return iter([])
    def close(self):
        pass

@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    # Reseta variáveis globais
    monkeypatch.setattr(consumer_mod, 'messages_store', [])
    monkeypatch.setattr(consumer_mod, 'stop_event', threading.Event())
    
    # Spy para upload_data
    calls = []
    def fake_upload(bucket, name, data):
        calls.append((bucket, name, data))
    monkeypatch.setattr(consumer_mod, 'upload_data', fake_upload)

    return {'calls': calls}

@patch('kafka.KafkaConsumer')
def test_consume_with_messages(mock_kafka_consumer, setup_env):
    # Configura o mock para retornar nosso FakeConsumer
    mock_kafka_consumer.return_value = FakeConsumer()

    # Executa a função
    consumer_mod.consume_kafka()

    # Verifica que as mensagens foram armazenadas
    assert consumer_mod.messages_store == [{'foo': 1}, {'bar': 2}]
    
    # Verifica chamadas de upload para MinIO
    calls = setup_env['calls']
    assert len(calls) == 2
    for bucket, name, data in calls:
        assert bucket == 'data'
        assert name.startswith('data_') and name.endswith('.parquet')
        # Header do Parquet ('PAR1')
        assert data[:4] == b'PAR1'

@patch('kafka.KafkaConsumer')
def test_consume_no_messages(mock_kafka_consumer, setup_env):
    # Configura o mock para retornar nosso EmptyConsumer
    mock_kafka_consumer.return_value = EmptyConsumer()

    # Não deve lançar exceção
    consumer_mod.consume_kafka()

    # Sem mensagens nem uploads
    assert consumer_mod.messages_store == []
    assert setup_env['calls'] == []
