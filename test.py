from sentence_transformers import SentenceTransformer
import psycopg2

# Load model sekali saja
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# PostgreSQL
conn = psycopg2.connect(
    dbname="mydb", user="myuser", password="mypassword", host="172.19.45.103", port="5432"
)

cur = conn.cursor()

query = "keuangan"
qvec = model.encode(query).tolist()

# Convert ke string format array
qvec_str = "[" + ",".join([str(x) for x in qvec]) + "]"

cur.execute(
    "SELECT content FROM documents ORDER BY embedding <-> %s::vector LIMIT 1",
    (qvec_str,)
)
print(cur.fetchone())
