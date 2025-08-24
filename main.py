from contextlib import asynccontextmanager
from fastapi import FastAPI
from service.qdrant_client import QdrantClientService
from service.rabbitmq_producer import RabbitmqProducer
from service.rabbitmq_consumer import RabbitmqConsumer
from routes.uploads import router as uploads_router
import logging
import threading
import os
from settings import (
    RABBITMQ_URL,
    RABBITMQ_SERVICE_NAME,
    LOCAL_STORAGE_PATH,
    VECTOR_DB_URL
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Application startup initiated.") 
    global qdrant_client, rabbitmq_producer, rabbitmq_consumer  

    if not os.path.exists(LOCAL_STORAGE_PATH):
        os.makedirs(LOCAL_STORAGE_PATH)
        logging.info(f"Created local storage directory at {LOCAL_STORAGE_PATH}.")

    try:
        qdrant_client = QdrantClientService(host=VECTOR_DB_URL, https=False)
        qdrant_client.connect()
    except Exception as e:
        logging.error(f"Failed to connect to Qdrant: {e}")
        raise

    try:
        rabbitmq_producer = RabbitmqProducer(rabbitmq_url=RABBITMQ_URL, service_name=RABBITMQ_SERVICE_NAME)
        rabbitmq_producer.connect()
    except Exception as e:
        logging.error(f"Failed to connect to RabbitMQ Producer: {e}")
        raise

    try:

        def start_consumer():
            rabbitmq_consumer = RabbitmqConsumer(rabbitmq_url=RABBITMQ_URL, service_name=RABBITMQ_SERVICE_NAME)
            rabbitmq_consumer.consume()
        
        consumer_thread = threading.Thread(target=start_consumer, daemon=True)
        consumer_thread.start()
        logging.info("RabbitMQ consumer started in background thread.")
    except Exception as e:
        logging.error(f"Failed to start RabbitMQ consumer: {e}")
        raise

    yield
    logging.info("Application shutdown initiated.")
    if rabbitmq_consumer:
        rabbitmq_consumer.close()
        logging.info("RabbitMQ consumer stopped.")
    if rabbitmq_producer:
        rabbitmq_producer.close()
        logging.info("RabbitMQ producer disconnected.")
    if qdrant_client:
        qdrant_client.disconnect()
        logging.info("Qdrant client disconnected.")

    logging.info("Application shutdown complete.")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Hello, World!"}

app.include_router(uploads_router, prefix="/uploads")