from typing import Dict
from pika import BlockingConnection, ConnectionParameters, BasicProperties, URLParameters, exceptions
import logging
import threading
import time
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RabbitmqProducer:
    def __init__(self, rabbitmq_url: str, service_name: str) -> None:
        self._rabbitmq_url = rabbitmq_url
        self._service_name = service_name
        self._heartbeat_interval = 60
        self._reconnect_delay = 5  # seconds
        self._connection = None
        self._channel = None

        self._lock = threading.Lock()

    def connect(self):
        try:
            logging.info("Connecting to RabbitMQ...")

            parameters = URLParameters(self._rabbitmq_url)
            parameters.heartbeat = self._heartbeat_interval

            self._connection = BlockingConnection(parameters)

            self._channel = self._connection.channel()

            self._channel.exchange_declare(
                exchange=self._service_name,
                exchange_type='topic',
                durable=True
            )

            self._channel.queue_declare(queue=self._service_name, durable=True)

            logging.info("Successfully connected to RabbitMQ Producer.")

        except exceptions.AMQPConnectionError as e:
            self._connection = None
            self._channel = None
            raise e


    def publish(self, message: Dict[str, any]) -> None:
        with self._lock:
            properties = BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json'
            )

            message_body = json.dumps(message)

            max_retries = 5
            for attemp in range(max_retries):
                try:
                    if not self._connection or not self._connection.is_open:
                        self.connect()

                    if self._channel and self._channel.is_open:
                        self._channel.basic_publish(
                            exchange=self._service_name,
                            routing_key=self._service_name,
                            body=message_body,
                            properties=properties
                        )
                        logging.info(f"Message published to RabbitMQ: {message}")
                        return
                    else:
                        logging.warning("Channel is not open, retrying...")
                except exceptions.AMQPConnectionError as e:
                    logging.error(f"Connection error: {e}, retrying in {self._reconnect_delay} seconds...")
                    time.sleep(self._reconnect_delay)
                except Exception as e:
                    logging.error(f"Failed to publish message: {e}")
                    raise e

    def close(self):
        if self._connection and self._connection.is_open:
            self._connection.close()
            logging.info("RabbitMQ Producer connection closed.")