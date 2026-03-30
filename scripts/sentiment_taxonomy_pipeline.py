import sqlite3
from scripts.custom_sentiment_taxonomy import CustomSentimentTaxonomy
from scripts.db_cache import save_sentiment_taxonomy

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


def main():

    engine = CustomSentimentTaxonomy()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT bank_id, bank_name, year, review_text
    FROM review_sentiments
    LIMIT 1000
    """)

    rows = cursor.fetchall()

    for _bank_id, bank, year, text in rows:

        emotion, category = engine.classify(text)

        save_sentiment_taxonomy(
            bank,
            year,
            text,
            emotion,
            category
        )

    conn.close()

    print("Custom sentiment taxonomy generated.")