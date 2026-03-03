import os
import sqlite3

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pdf_cache (
            file_path TEXT PRIMARY KEY,
            last_modified REAL,
            year INTEGER,
            score REAL
        )
    """)

    conn.commit()
    conn.close()


def get_file_modified_time(path):
    return os.path.getmtime(path)


def get_cached_score(file_path):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT last_modified, year, score FROM pdf_cache WHERE file_path=?",
        (file_path,)
    )

    row = cursor.fetchone()
    conn.close()

    return row


def update_cache(file_path, last_modified, year, score):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO pdf_cache
        (file_path, last_modified, year, score)
        VALUES (?, ?, ?, ?)
    """, (file_path, last_modified, year, score))

    conn.commit()
    conn.close()