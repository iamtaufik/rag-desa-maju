from typing import Dict, Union
from pika import URLParameters, BlockingConnection, exceptions
from concurrent.futures import ThreadPoolExecutor
from core.document_processor import DocumentProcessor
from core.minio import FileProcessor
import logging
import time
import functools
import os
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RabbitmqConsumer:
    def __init__(self, rabbitmq_url: str, service_name: str) -> None:
        self._rabbitmq_url = rabbitmq_url
        self._service_name = service_name
        self._heartbeat_interval = 300
        self._reconnect_delay = 5  # seconds
        self._connection = None
        self._channel = None
        self._is_ruinning = False

        self._executor = ThreadPoolExecutor(max_workers=3)

    def connect(self):
        try:
            logging.info("Connecting to RabbitMQ Consumer...")

            parameters = URLParameters(self._rabbitmq_url)
            parameters.heartbeat = self._heartbeat_interval
            parameters.blocked_connection_timeout = 600  # Allow some extra time for blocking

            self._connection = BlockingConnection(parameters)
            self._channel = self._connection.channel()

            self._channel.exchange_declare(
                exchange=self._service_name,
                exchange_type='topic',
                durable=True
            )
            self._channel.queue_declare(queue=self._service_name, durable=True)
            self._channel.queue_bind(
                exchange=self._service_name,
                queue=self._service_name,
                routing_key=self._service_name
            )
            logging.info("Successfully connected to RabbitMQ Consumer.")
            return True
        
        except exceptions.AMQPConnectionError as e:
            logging.error(f"Failed to connect to RabbitMQ Consumer: {e}")
            self._connection = None
            self._channel = None
            return False

    def consume(self):
        # Placeholder for RabbitMQ consumption logic
        if not self.connect():
            return
        
        self.is_running = True
        logging.info(f"Consuming messages from queue: {self._service_name}. Press CTRL+C to exit.")
        
        # Mengatur prefetch_count untuk mengelola pesan
        # self._channel.basic_qos(prefetch_count=1)

        # Mulai mengkonsumsi pesan dari queue
        self._channel.basic_consume(
            queue=self._service_name,
            on_message_callback=self._on_message_received,
            auto_ack=False # Jangan auto-acknowledge
        )
        
        # Mulai loop konsumsi
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("Consumer stopped by user.")
        finally:
            self.close()

    def _ack_message(self, delivery_tag):
        if self._channel and self._channel.is_open:
            self._channel.basic_ack(delivery_tag=delivery_tag)
            logging.info(f"Message acknowledged with delivery tag: {delivery_tag}")
        else:
            logging.warning("Channel is not open, cannot acknowledge message.")

    def _nack_message(self, delivery_tag, requeue=False):
        if self._channel and self._channel.is_open:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
            logging.info(f"Message not acknowledged with delivery tag: {delivery_tag}, requeue={requeue}")
        else:
            logging.warning("Channel is not open, cannot not acknowledge message.")

    def _threaded_callback(self, channel, method, properties, body):
        def do_work():
            try:
                logging.info(f"Processing message in thread: {body.decode('utf-8')}")
                self._on_message_received(channel, method, properties, body)

                cb = functools.partial(self._ack_message, channel, method.delivery_tag)
                self._connection.add_callback_threadsafe(cb)
            except Exception as e:
                logging.error(f"Error in threaded callback: {e}")
                cb = functools.partial(self._nack_message, channel, method.delivery_tag, requeue=False)
                self._connection.add_callback_threadsafe(cb)
        
        self._executor.submit(do_work)


    def _on_message_received(self, channel, method, properties, body):
        try:
            # Mengubah body pesan dari bytes ke string
            message = body.decode('utf-8')
            logging.info(f"Received message: {message}")
            
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON message: {e}")
                    return

            # Panggil metode untuk memproses pesan
            # raise Exception("Simulated error for testing purposes")  # Simulasi error untuk testing
            self.process_message(message)
            
            # Mengirim 'acknowledgement' setelah pesan berhasil diproses
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logging.info(f"Message acknowledged: {message}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Jika terjadi error, kita bisa menolak pesan (optional)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


    def process_message(self, message: Dict[str, any]):
        logging.info(f"Processing message: {message}")
        try:
            processor = DocumentProcessor()
            local_temp_path = None
            try:

                try:
                    file_processor = FileProcessor()
                    file = file_processor.download_from_minio_to_local(message['file_name'])
                except Exception as e:
                    logging.error(f"Failed to download file from MinIO: {e}")
                    return
                
                from main import qdrant_client
                
                local_temp_path = file['local_path']

                processed_data = processor.process(local_temp_path, message['file_name'])
                
                start_id = qdrant_client.get_next_id()

                points = qdrant_client.create_points(processed_data, start_id)

                if points:
                    qdrant_client.insert_points(points)
                    logging.info(f"Processed and inserted {len(points)} points into Qdrant.")

                os.unlink(local_temp_path)  # Hapus file lokal setelah diproses

            except Exception as e:
                logging.error(f"Failed Data Indexing {e}")
                os.unlink(local_temp_path)
                return
            
        except Exception as e:
            logging.error(f"Failed to initialize DocumentProcessor: {e}")
            return


    def close(self):
        if self._connection and self._connection.is_open:
            self._connection.close()
            logging.info("RabbitMQ Consumer connection closed.")