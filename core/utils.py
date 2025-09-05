import re

def classify_intent(query: str) -> str:
    # Regex/keyword untuk deteksi intent DB
    db_keywords = ["prediksi", "produk", "revenue", "penjualan", "analisis", "forecast"]
    if any(re.search(rf"\b{k}\b", query.lower()) for k in db_keywords):
        return "db"
    return "document"

def clean_sql(sql_query: str) -> str:
    # Hapus blok markdown ```sql ... ```
    sql_query = re.sub(r"```sql", "", sql_query, flags=re.IGNORECASE)
    sql_query = sql_query.replace("```", "")
    return sql_query.strip()