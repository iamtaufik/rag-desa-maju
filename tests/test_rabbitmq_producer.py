import pytest
from unittest.mock import patch, MagicMock
from pika import exceptions
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import service.rabbitmq_producer as rabbitmq_producer_module

@patch("service.rabbitmq_producer.BlockingConnection")
@patch("service.rabbitmq_producer.URLParameters")
def test_connect_success(mock_url_params, mock_blocking_conn):
    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_conn.channel.return_value = mock_channel
    mock_blocking_conn.return_value = mock_conn

    producer = rabbitmq_producer_module.RabbitmqProducer("amqp://guest:guest@localhost:5672/", "test_service")
    producer.connect()

    mock_url_params.assert_called_once_with("amqp://guest:guest@localhost:5672/")
    mock_blocking_conn.assert_called_once()
    mock_conn.channel.assert_called_once()
    mock_channel.exchange_declare.assert_called_once()
    mock_channel.queue_declare.assert_called_once()

@patch("service.rabbitmq_producer.BlockingConnection", side_effect=exceptions.AMQPConnectionError("fail"))
@patch("service.rabbitmq_producer.URLParameters")
def test_connect_fail(mock_url_params, mock_blocking_conn):
    producer = rabbitmq_producer_module.RabbitmqProducer("amqp://invalid", "test_service")

    with pytest.raises(exceptions.AMQPConnectionError):
        producer.connect()

@patch("service.rabbitmq_producer.BlockingConnection")
@patch("service.rabbitmq_producer.URLParameters")
def test_publish_success(mock_url_params, mock_blocking_conn):
    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_conn.channel.return_value = mock_channel
    mock_conn.is_open = True
    mock_channel.is_open = True
    mock_blocking_conn.return_value = mock_conn

    producer = rabbitmq_producer_module.RabbitmqProducer("amqp://guest:guest@localhost:5672/", "test_service")
    producer._connection = mock_conn
    producer._channel = mock_channel

    message = {"msg": "hello"}
    producer.publish(message)

    mock_channel.basic_publish.assert_called_once()

@patch("service.rabbitmq_producer.BlockingConnection", side_effect=RuntimeError("unexpected error"))
@patch("service.rabbitmq_producer.URLParameters")
def test_publish_fail_unexpected_error(mock_url_params, mock_blocking_conn):
    producer = rabbitmq_producer_module.RabbitmqProducer(
        "amqp://guest:guest@localhost:5672/", "test_service"
    )

    producer._connection = None
    producer._channel = None

    with pytest.raises(RuntimeError, match="unexpected error"):
        producer.publish({"msg": "fail"})

def test_close_success():
    producer = rabbitmq_producer_module.RabbitmqProducer("amqp://guest:guest@localhost:5672/", "test_service")

    mock_conn = MagicMock()
    mock_conn.is_open = True
    producer._connection = mock_conn

    producer.close()
    mock_conn.close.assert_called_once()

def test_close_no_connection():
    producer = rabbitmq_producer_module.RabbitmqProducer("amqp://guest:guest@localhost:5672/", "test_service")
    producer._connection = None

    # Tidak ada error walau tidak ada koneksi
    producer.close()