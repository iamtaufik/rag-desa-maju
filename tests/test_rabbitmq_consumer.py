# tests/test_rabbitmq_consumer.py
import pytest
from unittest.mock import patch, MagicMock
from pika import exceptions
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import service.rabbitmq_consumer as rabbitmq_consumer_module

rabbitmq_consumer_module.qdrant_client = MagicMock()

@patch("service.rabbitmq_consumer.BlockingConnection")
@patch("service.rabbitmq_consumer.URLParameters")
def test_connect_success(mock_url_params, mock_blocking_conn):
    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_conn.channel.return_value = mock_channel
    mock_blocking_conn.return_value = mock_conn

    consumer = rabbitmq_consumer_module.RabbitmqConsumer(
        "amqp://guest:guest@localhost:5672/", "test_service"
    )
    result = consumer.connect()

    assert result is True
    assert consumer._connection == mock_conn
    assert consumer._channel == mock_channel
    mock_channel.exchange_declare.assert_called_once()
    mock_channel.queue_declare.assert_called_once()
    mock_channel.queue_bind.assert_called_once()


@patch("service.rabbitmq_consumer.BlockingConnection", side_effect=exceptions.AMQPConnectionError("fail"))
@patch("service.rabbitmq_consumer.URLParameters")
def test_connect_fail(mock_url_params, mock_blocking_conn):
    consumer = rabbitmq_consumer_module.RabbitmqConsumer(
        "amqp://invalid", "test_service"
    )
    result = consumer.connect()
    assert result is False
    assert consumer._connection is None
    assert consumer._channel is None


def test_on_message_received_success():
    consumer = rabbitmq_consumer_module.RabbitmqConsumer("url", "queue")

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 123
    body = b'{"file_name": "dummy.pdf"}'

    with patch.object(consumer, "process_message") as mock_process:
        consumer._on_message_received(mock_channel, mock_method, None, body)

    mock_process.assert_called_once_with({"file_name": "dummy.pdf"})
    mock_channel.basic_ack.assert_called_once_with(delivery_tag=123)


def test_on_message_received_invalid_json():
    consumer = rabbitmq_consumer_module.RabbitmqConsumer("url", "queue")

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 123
    body = b'invalid-json'

    consumer._on_message_received(mock_channel, mock_method, None, body)

    mock_channel.basic_ack.assert_not_called()
    mock_channel.basic_nack.assert_not_called()


def test_on_message_received_fail_processing():
    consumer = rabbitmq_consumer_module.RabbitmqConsumer("url", "queue")

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 123
    body = b'{"file_name": "dummy.pdf"}'

    with patch.object(consumer, "process_message", side_effect=Exception("boom")):
        consumer._on_message_received(mock_channel, mock_method, None, body)

    mock_channel.basic_nack.assert_called_once_with(delivery_tag=123, requeue=False)

@patch("service.rabbitmq_consumer.FileProcessor")
def test_process_message_fail_download(mock_file_proc):
    consumer = rabbitmq_consumer_module.RabbitmqConsumer("url", "queue")
    mock_file_proc.return_value.download_from_minio_to_local.side_effect = Exception("MinIO down")

    consumer.process_message({"file_name": "missing.pdf"})


def test_close_success():
    consumer = rabbitmq_consumer_module.RabbitmqConsumer("url", "queue")
    mock_conn = MagicMock()
    mock_conn.is_open = True
    consumer._connection = mock_conn

    consumer.close()
    mock_conn.close.assert_called_once()


def test_close_no_connection():
    consumer = rabbitmq_consumer_module.RabbitmqConsumer("url", "queue")
    consumer._connection = None

    consumer.close()
