import google.generativeai as genai
from qdrant_client import QdrantClient
from qdrant_client.http.models import NamedVector
from settings import (
    GOOGLE_API_KEY,
    VECTOR_COLLECTION_NAME,
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
genai.configure(api_key=GOOGLE_API_KEY)
class DocumentRetrieval:
    def __init__(self, vector_db_client: QdrantClient):
        self.vector_db_client = vector_db_client

    def retrieve(self, query, top_k=10):
        # Generate embedding for the query using Google Generative AI
        query_embedding = genai.embed_content(
            model="models/text-embedding-004",
            content=query,
            task_type="retrieval_query"
        )["embedding"]

        # Search in the vector database
        hits = self.vector_db_client.search(
            collection_name=VECTOR_COLLECTION_NAME,
            query_vector=NamedVector(
                name="models/text-embedding-004", 
                vector=query_embedding
            ),
            limit=top_k
        )

        logging.info(f"Retrieved {len(hits)} documents for the query.")
        # Extract and return the relevant documents
        documents = [
            {
                "document": hit.payload["document"],
                "filename": hit.payload.get("filename"),
                "page_number": hit.payload.get("page_number")
            }
            for hit in hits
        ]
        return documents
    
    def retrieve_hybrid(self, query, top_k=10):
        hits = self.vector_db_client.hybrid_search(query=query, limit=top_k)

        documents = [
            {
                "document": hit.payload["document"],
                "filename": hit.payload.get("filename"),
                "page_number": hit.payload.get("page_number")
            }
            for hit in hits
        ]
        return documents
    
    def generate_response_stream(self, query, documents):
        # Combine the query with the retrieved documents to form a prompt
        context = "\n".join([
            f"[{doc['filename']} - Page {doc['page_number']}]\n{doc['document']}"
            for doc in documents
        ])

        prompt = f"""
        Kamu adalah asisten AI yang menjawab pertanyaan berdasarkan dokumen.

        Gunakan hanya informasi dari konteks berikut. 
        Jika jawabannya tidak ada, balas dengan: "Maaf, saya tidak menemukan jawaban di dokumen."

        Format jawaban WAJIB seperti ini:

        [ringkasan jawaban]

        sumber:
        1. [nama_file] halaman [page_number]
        2. [nama_file] halaman [page_number]

        Pertanyaan:
        {query}

        Konteks:
        {context}
        """

        # Generate a response using Google Generative AI
        stream = genai.GenerativeModel("gemini-1.5-flash").generate_content(prompt, stream=True)
        for chunk in stream:
                if chunk.candidates and chunk.candidates[0].content.parts:
                    yield chunk.candidates[0].content.parts[0].text