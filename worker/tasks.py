import logging
from celery import Celery
from settings import RABBITMQ_URL, REDIS_URL
from core.document_processor import DocumentProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Celery('tasks', broker=RABBITMQ_URL, backend=REDIS_URL)

@app.task
def add(x, y):
    return x + y

@app.task
def document_processing_task(doc_info: dict):
    try:
        processor = DocumentProcessor()
        local_path = processor.download_file_to_local(doc_info['file_name'])
        processor.process(file_path=local_path, filename=doc_info['file_name'])
        return {"status": "success", "message": "Document processed successfully."}
    except Exception as e:
        logger.error(f"Failed to process document {doc_info['file_name']}: {e}")
        return {"status": "error", "message": str(e)}
