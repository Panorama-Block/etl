import threading
import pytest
from unittest.mock import patch, MagicMock
import json
import time
import signal

import src.pipeline.core.kafka_multi_consumer as multi_consumer

# Classes fake para simular mensagens Kafka
class FakeMessage:
    def __init__(self, value):
        self.value = value

class FakeConsumer:
    def __init__(self, messages=None, stop_event=None):
        self.messages = messages or []
        self._stop = False
        self.stop_event = stop_event
    
    def __iter__(self):
        while not self._stop and (not self.stop_event or not self.stop_event.is_set()):
            if self.messages:
                yield from self.messages
            time.sleep(0.1)  # Small delay to prevent CPU spinning
    
    def close(self):
        self._stop = True

@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    # Reset global variables
    monkeypatch.setattr(multi_consumer, 'consumer_threads', {})
    monkeypatch.setattr(multi_consumer, 'stop_events', {})
    
    # Mock upload_data function
    calls = []
    def fake_upload(bucket, name, data):
        calls.append((bucket, name, data))
    monkeypatch.setattr(multi_consumer, 'upload_data', fake_upload)
    
    return {'calls': calls}

def test_start_all_consumers():
    """Test if all consumers are started correctly"""
    # Create a stop event that will be used by all consumers
    global_stop_event = threading.Event()
    
    def mock_kafka_consumer(*args, **kwargs):
        return FakeConsumer(stop_event=global_stop_event)
    
    with patch('kafka.KafkaConsumer', side_effect=mock_kafka_consumer):
        try:
            multi_consumer.start_all_consumers()
            
            # Give threads a moment to start
            time.sleep(0.1)
            
            # Check if threads were created for each topic
            assert len(multi_consumer.consumer_threads) == len(multi_consumer.TOPICS)
            assert len(multi_consumer.stop_events) == len(multi_consumer.TOPICS)
            
            # Check if all threads are running
            for thread in multi_consumer.consumer_threads.values():
                assert thread.is_alive()
                assert thread.daemon  # Should be daemon threads
        finally:
            # Ensure cleanup happens even if test fails
            global_stop_event.set()
            multi_consumer.stop_all_consumers()
            
            # Wait for all threads to finish with timeout
            for thread in multi_consumer.consumer_threads.values():
                thread.join(timeout=1.0)
            
            # Double check all threads are stopped
            for thread in multi_consumer.consumer_threads.values():
                assert not thread.is_alive()

@pytest.mark.timeout(5)  # Add timeout to the entire test
def test_stop_all_consumers():
    """Test if all consumers are stopped correctly"""
    # Create a stop event that will be used by all consumers
    global_stop_event = threading.Event()
    
    def mock_kafka_consumer(*args, **kwargs):
        return FakeConsumer(stop_event=global_stop_event)
    
    with patch('kafka.KafkaConsumer', side_effect=mock_kafka_consumer):
        try:
            # Start consumers first
            multi_consumer.start_all_consumers()
            time.sleep(0.1)  # Give threads time to start
            
            # Stop all consumers
            global_stop_event.set()  # Set global stop event first
            multi_consumer.stop_all_consumers()
            
            # Wait for all threads to finish with timeout
            for thread in multi_consumer.consumer_threads.values():
                thread.join(timeout=1.0)
            
            # Check if all stop events are set
            for stop_event in multi_consumer.stop_events.values():
                assert stop_event.is_set()
            
            # Check if all threads are finished
            for thread in multi_consumer.consumer_threads.values():
                assert not thread.is_alive()
        finally:
            # Ensure cleanup happens even if test fails
            global_stop_event.set()
            multi_consumer.stop_all_consumers()
            
            # Force stop all threads
            for thread in multi_consumer.consumer_threads.values():
                if thread.is_alive():
                    thread.join(timeout=0.1)
                    if thread.is_alive():
                        # If thread is still alive after timeout, force terminate
                        import ctypes
                        thread_id = thread.ident
                        if thread_id is not None:
                            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                                ctypes.c_long(thread_id),
                                ctypes.py_object(SystemExit)
                            )
                            if res == 0:
                                raise ValueError("Invalid thread ID")
                            elif res != 1:
                                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                                    ctypes.c_long(thread_id),
                                    None
                                )
                                raise SystemError("PyThreadState_SetAsyncExc failed")

@patch('kafka.KafkaConsumer')
def test_consume_topic_with_chain_data(mock_kafka_consumer, setup_env):
    """Test consuming messages with chain data"""
    # Create test message
    chain_data = {
        "type": "chain_data",
        "chain": {
            "id": "test_chain",
            "name": "Test Chain",
            "status": "active"
        }
    }
    
    stop_event = threading.Event()
    
    # Setup mock consumer with test message
    mock_kafka_consumer.return_value = FakeConsumer(
        messages=[FakeMessage(json.dumps(chain_data))],
        stop_event=stop_event
    )
    
    # Run consumer for a specific topic
    topic = "chains_topic"
    bucket = multi_consumer.TOPICS[topic]
    
    # Start consumer in a separate thread
    thread = threading.Thread(
        target=multi_consumer.consume_topic,
        args=(topic, bucket, stop_event),
        daemon=True
    )
    thread.start()
    
    try:
        # Give it some time to process
        time.sleep(0.1)
        
        # Stop the consumer
        stop_event.set()
        thread.join(timeout=1)
        
        # Verify upload was called with correct data
        calls = setup_env['calls']
        assert len(calls) == 1
        bucket_name, file_name, data = calls[0]
        assert bucket_name == bucket
        assert file_name.startswith(topic)
        assert file_name.endswith('.parquet')
    finally:
        # Ensure thread is stopped
        stop_event.set()
        thread.join(timeout=1)

@patch('kafka.KafkaConsumer')
def test_consume_topic_with_block_data(mock_kafka_consumer, setup_env):
    """Test consuming messages with block data"""
    # Create test message
    block_data = {
        "type": "block_data",
        "block": {
            "number": 12345,
            "hash": "0x123",
            "timestamp": 1234567890
        }
    }
    
    stop_event = threading.Event()
    
    # Setup mock consumer with test message
    mock_kafka_consumer.return_value = FakeConsumer(
        messages=[FakeMessage(json.dumps(block_data))],
        stop_event=stop_event
    )
    
    # Run consumer for a specific topic
    topic = "blocks_topic"
    bucket = multi_consumer.TOPICS[topic]
    
    # Start consumer in a separate thread
    thread = threading.Thread(
        target=multi_consumer.consume_topic,
        args=(topic, bucket, stop_event),
        daemon=True
    )
    thread.start()
    
    try:
        # Give it some time to process
        time.sleep(0.1)
        
        # Stop the consumer
        stop_event.set()
        thread.join(timeout=1)
        
        # Verify upload was called with correct data
        calls = setup_env['calls']
        assert len(calls) == 1
        bucket_name, file_name, data = calls[0]
        assert bucket_name == bucket
        assert file_name.startswith(topic)
        assert file_name.endswith('.parquet')
    finally:
        # Ensure thread is stopped
        stop_event.set()
        thread.join(timeout=1)

@patch('kafka.KafkaConsumer')
def test_consume_topic_with_invalid_data(mock_kafka_consumer, setup_env):
    """Test handling of invalid message data"""
    # Create invalid test message
    invalid_data = {
        "type": "invalid_type",
        "data": "some data"
    }
    
    stop_event = threading.Event()
    
    # Setup mock consumer with invalid message
    mock_kafka_consumer.return_value = FakeConsumer(
        messages=[FakeMessage(json.dumps(invalid_data))],
        stop_event=stop_event
    )
    
    # Run consumer for a specific topic
    topic = "chains_topic"
    bucket = multi_consumer.TOPICS[topic]
    
    # Start consumer in a separate thread
    thread = threading.Thread(
        target=multi_consumer.consume_topic,
        args=(topic, bucket, stop_event),
        daemon=True
    )
    thread.start()
    
    try:
        # Give it some time to process
        time.sleep(0.1)
        
        # Stop the consumer
        stop_event.set()
        thread.join(timeout=1)
        
        # Verify no upload was made for invalid data
        calls = setup_env['calls']
        assert len(calls) == 0
    finally:
        # Ensure thread is stopped
        stop_event.set()
        thread.join(timeout=1) 