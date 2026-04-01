import sqlite3
import os

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


def generate_narrative_scores():

    if not os.path.exists(DB_PATH):
        print("Database not found")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT bank_name, year, AVG(doc_mean_signed) AS avg_doc_signed
        FROM corporate_document_sentiment_rollup
        GROUP BY bank_name, year
        """
    )

    rows = cursor.fetchall()

    for bank_name, year, avg_doc_signed in rows:
        cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
        row = cursor.fetchone()
        if not row:
            continue
        bank_id = row[0]

        if avg_doc_signed is None:
            continue

        narrative_score = round(((float(avg_doc_signed) + 1.0) / 2.0) * 100.0, 2)

        cursor.execute(
            """
            INSERT OR REPLACE INTO narrative_scores
            (bank_id, bank_name, year, score)
            VALUES (?, ?, ?, ?)
            """,
            (bank_id, bank_name, int(year), narrative_score),
        )

    conn.commit()
    conn.close()

    print("Narrative scores generated successfully")


if __name__ == "__main__":
    generate_narrative_scores()