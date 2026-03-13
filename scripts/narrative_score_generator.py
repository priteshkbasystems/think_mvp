import sqlite3
import os

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


def generate_narrative_scores():

    if not os.path.exists(DB_PATH):
        print("Database not found")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_path, year, score
        FROM pdf_cache
    """)

    rows = cursor.fetchall()

    for file_path, year, score in rows:

        bank_name = file_path.split("/")[-4].replace("_"," ")

        narrative_score = round(score * 100)

        cursor.execute("""
        INSERT OR REPLACE INTO narrative_scores
        (bank_name, year, score)
        VALUES (?, ?, ?)
        """, (bank_name, year, narrative_score))

    conn.commit()
    conn.close()

    print("Narrative scores generated successfully")


if __name__ == "__main__":
    generate_narrative_scores()