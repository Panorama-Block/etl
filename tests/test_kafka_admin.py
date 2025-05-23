import pytest
from confluent_kafka.admin import NewTopic
from src.pipeline.core import kafka_admin

# Um “fake” simples que simula o retorno de admin_client.list_topics()
class FakeMetadata:
    def __init__(self, topics_dict):
        # topics_dict: e.g. {'chains_topic': None, 'other': None}
        self.topics = topics_dict

@pytest.fixture(autouse=True)
def reset_admin_client(monkeypatch):
    """
    Garante que, a cada teste, admin_client seja sempre o mesmo objeto,
    e não quebras entre testes.
    """
    # Recomenda-se importar o admin_client *de dentro* do seu módulo
    yield

def test_create_topic_when_missing(monkeypatch, capsys):
    # 1) lista só “other”, não há chains_topic
    fake_meta = FakeMetadata({'other': None})
    monkeypatch.setattr(kafka_admin.admin_client, 'list_topics', lambda timeout: fake_meta)

    created = []
    def fake_create(topics):
        created.extend(topics)
    monkeypatch.setattr(kafka_admin.admin_client, 'create_topics', fake_create)

    # 2) chama a função
    kafka_admin.create_topic()

    # 3) verificações
    assert len(created) == 1
    topic_obj = created[0]
    assert isinstance(topic_obj, NewTopic)
    assert topic_obj.topic == 'chains_topic'
    # confere também num_partitions e replication_factor
    assert topic_obj.num_partitions == 1
    assert topic_obj.replication_factor == 1

    # 4) verifica o print()
    out = capsys.readouterr().out
    assert "🆕 Tópico 'chains_topic' criado com sucesso!" in out

def test_create_topic_when_exists(monkeypatch, capsys):
    # simula que já existe
    fake_meta = FakeMetadata({'chains_topic': None})
    monkeypatch.setattr(kafka_admin.admin_client, 'list_topics', lambda timeout: fake_meta)

    # spy para create_topics
    called = []
    monkeypatch.setattr(kafka_admin.admin_client, 'create_topics', lambda topics: called.append(topics))

    kafka_admin.create_topic()

    # não deve ter chamado create_topics
    assert called == []
    out = capsys.readouterr().out
    assert "🔶 Tópico 'chains_topic' já existe." in out

def test_create_all_topics_when_all_missing(monkeypatch, capsys):
    # nenhum tópico existe
    fake_meta = FakeMetadata({})
    monkeypatch.setattr(kafka_admin.admin_client, 'list_topics', lambda timeout: fake_meta)

    created = []
    monkeypatch.setattr(kafka_admin.admin_client, 'create_topics', lambda topics: created.extend(topics))

    kafka_admin.create_all_topics()

    # deve criar exatamente len(TOPICS) NewTopic
    assert len(created) == len(kafka_admin.TOPICS)
    for nt in created:
        assert isinstance(nt, NewTopic)
        assert nt.topic in kafka_admin.TOPICS

    # verifica prints para cada tópico
    out = capsys.readouterr().out
    for topic_name in kafka_admin.TOPICS:
        assert f"🆕 Tópico '{topic_name}' criado com sucesso!" in out

def test_create_all_topics_when_some_exist(monkeypatch, capsys):
    # simula que metadados já têm metade dos tópicos
    half = list(kafka_admin.TOPICS.keys())[:len(kafka_admin.TOPICS)//2]
    existing = {name: None for name in half}
    fake_meta = FakeMetadata(existing)
    monkeypatch.setattr(kafka_admin.admin_client, 'list_topics', lambda timeout: fake_meta)

    created = []
    monkeypatch.setattr(kafka_admin.admin_client, 'create_topics', lambda topics: created.extend(topics))

    kafka_admin.create_all_topics()

    # só cria os que faltam
    assert len(created) == len(kafka_admin.TOPICS) - len(existing)
    out = capsys.readouterr().out
    for name in half:
        assert f"🔶 Tópico '{name}' já existe." in out
    for name in kafka_admin.TOPICS:
        if name not in existing:
            assert f"🆕 Tópico '{name}' criado com sucesso!" in out