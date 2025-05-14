# tests/conftest.py
import sys
import types
import pytest

@pytest.fixture(autouse=True)
def stub_kafka(monkeypatch):
    """
    Antes de qualquer import do seu código, coloca um módulo 'kafka' fake
    no sys.modules para que from kafka import KafkaConsumer
    resolva aqui, e não na kafka-python real.
    """
    fake_kafka = types.ModuleType("kafka")
    # define um stub de KafkaConsumer — nos testes você vai sobrescrever
    fake_kafka.KafkaConsumer = lambda *args, **kwargs: None

    # injeta esse fake no sistema de módulos
    monkeypatch.setitem(sys.modules, "kafka", fake_kafka)