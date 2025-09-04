import os
from dotenv import load_dotenv

# Load file .env
load_dotenv()

VECTOR_DB_URL = os.getenv("VECTOR_DB_URL", "localhost")
VECTOR_COLLECTION_NAME = os.getenv("VECTOR_COLLECTION_NAME", "default")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "rabbitmq")
RABBITMQ_SERVICE_NAME = os.getenv("RABBITMQ_SERVICE_NAME", "desa-maju-rag-service")

BUCKET_NAME = "rag-bucket-desa-maju"
LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH", "storage")

MINIO_HOST = os.getenv("MINIO_HOST", "minio")
MINIO_PORT = int(os.getenv("MINIO_PORT", "9000"))

GEMINI_EMBEDDING_MODEL = "models/text-embedding-004"
MODEL_ID = "nomic-ai/nomic-embed-text-v2-moe"
MODEL_NAME = "nomic-embed-text-v2-moe"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    ""
)


