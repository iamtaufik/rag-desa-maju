import asyncio
import google.generativeai as genai
from service.qdrant_client import QdrantClientService
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from qdrant_client.http.models import NamedVector
from .utils import classify_intent, clean_sql
from settings import (
    GOOGLE_API_KEY,
    VECTOR_COLLECTION_NAME,
    DATABASE_URL
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
genai.configure(api_key=GOOGLE_API_KEY)
class DocumentRetrieval:
    def __init__(self, vector_db_client: QdrantClientService):
        self.vector_db_client = vector_db_client
        self.db_engine = create_async_engine(DATABASE_URL) if DATABASE_URL else None

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
    
    async def retrieve_from_db(self, query: str):
        prompt = f"""
        Kamu adalah asisten SQL.
        Pertanyaan user: "{query}"
        Database schema:
        Tabel predictive_analis(name, prediction_revenue)

        Buat SQL valid untuk PostgreSQL:
        - Gunakan kolom "name"
        - Untuk pencarian string, gunakan ILIKE dengan wildcard, contoh:
        SELECT ... WHERE name ILIKE '%produk%';
        - Jangan gunakan format markdown, cukup SQL murni.
        """

        raw_sql = genai.GenerativeModel("gemini-1.5-flash").generate_content(prompt).text.strip()
        logging.info(f"Generated SQL: {raw_sql}")
        sql_query = clean_sql(raw_sql)
        logging.info(f"Cleaned SQL: {sql_query}")
 
        if not self.db_engine:
            raise ValueError("Database engine belum dikonfigurasi")

        async with AsyncSession(self.db_engine) as session:
            result = await session.execute(text(sql_query))
            rows = result.fetchall()

        logging.info(f"Executed SQL: {sql_query} with {rows} rows returned.")
        return {"sql": sql_query, "rows": rows}
    
    async def answer_query(self, query, top_k=10):
        intent = classify_intent(query)  # atau classify_intent_llm(query)

        if intent == "db":
            db_result = await self.retrieve_from_db(query)
            return self.generate_response_stream(query, db_result=db_result)
        else:
            documents = self.retrieve_hybrid(query, top_k=top_k)
            return self.generate_response_stream(query, documents=documents)

    async def generate_response_stream(self, query, documents=None, db_result=None):
        # Build context dari dokumen kalau ada
        context_parts = []
        if documents:
            context_parts.append("\n".join([
                f"[{doc['filename']} - Page {doc['page_number']}]\n{doc['document']}"
                for doc in documents
            ]))

        # Build context dari hasil DB kalau ada
        if db_result:
            rows_str = "\n".join([str(row) for row in db_result["rows"]])
            context_parts.append(f"Hasil query database:\nSQL: {db_result['sql']}\nData:\n{rows_str}")

        context = "\n\n".join(context_parts)

        # Prompt fleksibel: dokumen + DB
        prompt = f"""
        Kamu adalah asisten AI yang menjawab pertanyaan berdasarkan konteks berikut.

        Gunakan hanya informasi yang tersedia di konteks. 
        Jika jawabannya tidak ada, balas dengan: "Maaf, saya tidak menemukan jawaban di dokumen/database."

        Format jawaban WAJIB seperti ini:

        [ringkasan jawaban]

        sumber:
        1. [nama_file] halaman [page_number]   <-- untuk dokumen
        atau
        1. [database: predictive_analis]       <-- untuk DB

        Hilangkan simbol [] pada jawaban dan sumber.

        Pertanyaan:
        {query}

        Konteks:
        {context}
        """

        # Streaming dari Gemini
        model = genai.GenerativeModel("gemini-1.5-flash")
        stream_coro = model.generate_content_async(prompt, stream=True)

        stream = await stream_coro

        # Sekarang bisa async for
        async for chunk in stream:
            if chunk.candidates and chunk.candidates[0].content.parts:
                yield chunk.candidates[0].content.parts[0].text
