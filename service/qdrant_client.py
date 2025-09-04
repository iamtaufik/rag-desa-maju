import google.generativeai as genai
from typing import Dict, List
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    MultiVectorConfig,
    SparseVectorParams,
    SparseIndexParams,
    PointStruct, 
    SparseVector
)
from qdrant_client.http.models import VectorParams, Distance
from fastembed import SparseTextEmbedding, LateInteractionTextEmbedding
from qdrant_client import QdrantClient
from datetime import datetime
from settings import (
    VECTOR_COLLECTION_NAME,
    GOOGLE_API_KEY,
    LOCAL_STORAGE_PATH
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

GEMINI_EMBEDDING_MODEL = "models/text-embedding-004"
GEMINI_VECTOR_SIZE = 768
COLBERT_VECTOR_SIZE = 128 # Ukuran vektor umum untuk ColbertV2
# Nama vektor yang akan digunakan di Qdrant
VECTOR_NAMES = {
    "gemini": GEMINI_EMBEDDING_MODEL,
    "colbert": "colbertv2",
    "bm25": "bm25"
}

genai.configure(api_key=GOOGLE_API_KEY)


class QdrantClientService:
    def __init__(self, host: str, https: bool = False):
        self._host = host
        self._https = https
        self._client = None
        self._genai = genai
        # Inisialisasi model BM25 dan Colbert jika belum
        self._bm25_model = SparseTextEmbedding(model_name="Qdrant/bm25", cache_dir=f"./{LOCAL_STORAGE_PATH}/models/bm25")
        self._colbert_model = LateInteractionTextEmbedding(model_name="colbert-ir/colbertv2.0", cache_dir=f"./{LOCAL_STORAGE_PATH}/models/colbert")


    def connect(self):
        try:
            self._client = QdrantClient(url=self._host, https=self._https, timeout=120)
            logging.info(f"Connected to Qdrant at {self._host}")
            self._ensure_collection()
            return True
        except Exception as e:
            logging.error(f"Failed to connect to Qdrant: {e}")
            self._client = None
            return False

    def _ensure_collection(self):
        try:
            collection = self._client.collection_exists(collection_name=VECTOR_COLLECTION_NAME)

            if not collection:
                self._create_collection()
            else:
                logging.info(f"Collection '{VECTOR_COLLECTION_NAME}' already exists.")
        
        except Exception as e:
            logging.error(f"Error checking/creating collection: {e}")
            raise e

    def _create_collection(self):
        try:
            self._client.create_collection(
                collection_name=VECTOR_COLLECTION_NAME,
                vectors_config={
                    VECTOR_NAMES["gemini"]: VectorParams(
                        size=GEMINI_VECTOR_SIZE, 
                        distance=Distance.COSINE
                    ),
                    VECTOR_NAMES["colbert"]: VectorParams(
                        size=COLBERT_VECTOR_SIZE,
                        distance=Distance.DOT, # Colbert menggunakan Dot Product
                        on_disk=True,
                        multivector_config=MultiVectorConfig(
                            comparator="max_sim" # Parameter yang diminta oleh Qdrant
                        )
                    )
                },
                sparse_vectors_config = {
                    VECTOR_NAMES["bm25"]: SparseVectorParams(
                        index=SparseIndexParams(
                            on_disk=True
                        )
                    )
                }
            )
            logging.info(f"Hybrid collection '{VECTOR_COLLECTION_NAME}' created successfully.")
        except Exception as e:
            logging.error(f"Failed to create/check collection: {e}")
            raise e
        
    def create_points(self, processed_data: Dict[str, any], start_id: int) -> List[PointStruct]:
        points = []
        for i, (bm25_vector, colbert_vector, gemini_vector, doc, chunk) in enumerate(zip(
            processed_data["bm25_vectors"],
            processed_data["colbert_vectors"],
            processed_data["gemini_vectors"],
            processed_data["docs"],
            processed_data["chunks"]
        )):
            page_no = None
            if chunk.meta.doc_items and chunk.meta.doc_items[0].prov:
                page_no = chunk.meta.doc_items[0].prov[0].page_no

            sparse_vector_qdrant = SparseVector(
                indices=bm25_vector.indices.tolist(),  # Konversi ke list
                values=bm25_vector.values.tolist()     # Konversi ke list
            )
            
            # 2. Konversi Vektor Colbert ke format yang diharapkan
            # Vektor Colbert dari fastembed adalah array NumPy 2D (list of lists)
            colbert_vector_list = colbert_vector.tolist()

            point = PointStruct(
                id=start_id + i,
                vector={
                    VECTOR_NAMES["bm25"]: sparse_vector_qdrant,
                    VECTOR_NAMES["colbert"]: colbert_vector_list,
                    VECTOR_NAMES["gemini"]: gemini_vector
                },
                payload={
                "document": doc,
                "filename": processed_data["filename"],
                "page_number": page_no,
                "upload_timestamp": datetime.utcnow().isoformat(),
                }
            )
            points.append(point)

        return points
    
    def get_next_id(self) -> int:
        try:
            collection_info = self._client.get_collection(collection_name=VECTOR_COLLECTION_NAME)
            return collection_info.points_count
        except Exception as e:
            logging.error(f"Failed to get next ID: {e}")
            return 0
    
    def insert_points(self, points: List[PointStruct], batch_size: int = 25):
        # logging.info(f"\n\nPoints: {points}\n\n")
        if not self._client:
            logging.error("Qdrant client is not connected.")
            return

        try:
            for i in range(0, len(points), batch_size):
                batch = points[i:i+batch_size]
                self._client.upsert(
                    collection_name=VECTOR_COLLECTION_NAME,
                    points=batch
                )
                logging.info(f"Inserted batch {i//batch_size + 1} with {len(batch)} points.")
        except Exception as e:
            logging.error(f"Failed to insert points: {e}")
            raise e


    def search_points(self, query: str, limit: int = 5) -> List[PointStruct]:
        logging.info(f"Performing hybrid search for query: '{query}'")

        try:
            query_gemini_vector = self._genai.embed_content(
                content=query,
                model=GEMINI_EMBEDDING_MODEL,
                task_type="retrieval_document"
            )['embedding']

            query_colbert_vectors = self._bm25_model.query_embed(query=query)
            sparse_vector_qdrant = SparseVector(
                indices=query_bm25_vector.indices.tolist(),
                values=query_bm25_vector.values.tolist()
            )
            # 3. Hasil Vektor BM25 (Sparse)
            query_bm25_vector = self._bm25_model.embed_documents(texts=[query])[0]
        except Exception as e:
            logging.error(f"Error embedding query: {e}")
            return []



    def disconnect(self):
        if self._client:
            try:
                self._client.close()
                logging.info("Disconnected from Qdrant.")
            except Exception as e:
                logging.error(f"Error during disconnection: {e}")
        else:
            logging.warning("No active Qdrant client to disconnect.")

    def insert_data(self, data):
        # Logic to insert data into Qdrant
        pass

    def query_data(self, query):
        # Logic to query data from Qdrant
        pass