from fastapi import APIRouter
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from core.retrival import DocumentRetrieval
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

router = APIRouter(tags=["Chat"])

@router.get("/chat/stream")
async def chat_stream(query: str):
    try:
        from main import qdrant_client

        if not qdrant_client or not qdrant_client.connect():
            return JSONResponse(content={"message": "Qdrant client is not connected."}, status_code=500)

        document_retrieval = DocumentRetrieval(vector_db_client=qdrant_client)
        documents = document_retrieval.retrieve_hybrid(query, top_k=10)

        def event_generator():
            for chunk_text in document_retrieval.generate_response_stream(query, documents):
                yield chunk_text

        return StreamingResponse(event_generator(), media_type="text/plain")
    except Exception as e:
        logging.error(f"Error processing chat request: {str(e)}")
        return JSONResponse(content={"message": f"Failed to process chat request: {str(e)}"}, status_code=500)
