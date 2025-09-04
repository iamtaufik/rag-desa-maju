import google.generativeai as genai
from settings import (
    GOOGLE_API_KEY
)
genai.configure(api_key=GOOGLE_API_KEY)
query = "Apa isi bab 2 dokumen ini?"

query_embedding = genai.embed_content(
    content=query,
    model="models/text-embedding-004",
    task_type="retrieval_query"
)["embedding"]


for hit in hits:
    print(hit.payload["text"], " (score:", hit.score, ")")
