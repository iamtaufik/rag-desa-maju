from sentence_transformers import SentenceTransformer
import psycopg2

# Load model sekali saja
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# PostgreSQL
conn = psycopg2.connect(
    dbname="mydb", user="myuser", password="mypassword", host="172.19.45.103", port="5432"
)
cur = conn.cursor()

# Simpan data
text = "Peraturan Desa tentang sistem keamanan desa dengan partisipasi masyarakat"
embedding = model.encode(text).tolist()
cur.execute("INSERT INTO documents (content, embedding) VALUES (%s, %s)", (text, embedding))
conn.commit()
