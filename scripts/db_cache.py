import os
import sqlite3
import hashlib
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


# ==========================================
# DATABASE CONNECTION (Reusable)
# ==========================================
def get_connection():
    return sqlite3.connect(DB_PATH)


# ==========================================
# DATABASE INITIALIZATION
# ==========================================
def init_db():

    conn = get_connection()
    cursor = conn.cursor()

    # ------------------------------
    # PDF CACHE
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pdf_cache (
            file_path TEXT PRIMARY KEY,
            last_modified REAL,
            year INTEGER,
            score REAL
        )
    """)

    # ------------------------------
    # PDF TEXT CACHE
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pdf_text_cache (
            file_path TEXT PRIMARY KEY,
            text TEXT
        )
    """)

    # ------------------------------
    # CORPORATE TOPIC CACHE
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS corporate_topic_cache (
            file_path TEXT PRIMARY KEY,
            last_modified REAL
        )
    """)

    # ------------------------------
    # BANKS
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS banks (
            bank_name TEXT PRIMARY KEY
        )
    """)

    # ------------------------------
    # SENTIMENT SCORES
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_scores (
            bank_name TEXT,
            year INTEGER,
            sentiment REAL,
            contradiction_ratio REAL,
            PRIMARY KEY (bank_name, year)
        )
    """)

    # ------------------------------
    # REVIEW SENTIMENTS
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS review_sentiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT,
            year INTEGER,
            review_text TEXT,
            topic_id INTEGER,
            review_hash TEXT UNIQUE,
            rating REAL,
            sentiment_score REAL,
            sentiment_label TEXT,
            review_source TEXT
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_review_hash
        ON review_sentiments(review_hash)
    """)

    # ------------------------------
    # COMPLAINT TOPICS
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS complaint_topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT,
            topic_id INTEGER,
            keywords TEXT,
            review_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------
    # EMBEDDINGS
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS embedding_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text_hash TEXT UNIQUE,
            embedding BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------
    # CORPORATE SENTIMENT
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS corporate_sentiment (
            bank_name TEXT,
            year INTEGER,
            sentiment REAL
        )
    """)

    # ------------------------------
    # CORPORATE TOPIC SENTIMENT
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS corporate_topic_sentiment (
            bank_name TEXT,
            year INTEGER,
            topic TEXT,
            sentiment REAL,
            PRIMARY KEY(bank_name, year, topic)
        )
    """)

    # ------------------------------
    # PIPELINE RUNS
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            step_name TEXT PRIMARY KEY,
            last_run TIMESTAMP,
            status TEXT
        )
    """)

    # ------------------------------
    # STEP PROGRESS
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS step_progress (
            step_name TEXT,
            bank_name TEXT,
            year INTEGER,
            last_processed_index INTEGER,
            PRIMARY KEY(step_name, bank_name, year)
        )
    """)

    # ------------------------------
    # STOCK RETURNS (RESTORED)
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_returns (
        bank_name TEXT,
        year INTEGER,
        return REAL,
        PRIMARY KEY (bank_name, year)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS financial_metrics (
    bank_name TEXT,
    year INTEGER,
    revenue REAL,
    net_profit REAL,
    operating_income REAL,
    total_assets REAL,
    roe REAL,
    PRIMARY KEY(bank_name, year)
    )
    """)
    conn.commit()
    conn.close()


# ==========================================
# FILE MODIFIED TIME
# ==========================================
def get_file_modified_time(path):
    return os.path.getmtime(path)


# ==========================================
# PDF CACHE
# ==========================================
def get_cached_score(file_path):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT last_modified, year, score
        FROM pdf_cache
        WHERE file_path=?
    """, (file_path,))

    row = cursor.fetchone()
    conn.close()

    return row


def update_cache(file_path, last_modified, year, score):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO pdf_cache
        VALUES (?, ?, ?, ?)
    """, (file_path, last_modified, year, score))

    conn.commit()
    conn.close()


# ==========================================
# PDF TEXT CACHE
# ==========================================
def get_cached_pdf_text(file_path):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT text FROM pdf_text_cache WHERE file_path=?
    """, (file_path,))

    row = cursor.fetchone()
    conn.close()

    return row[0] if row else None


def save_pdf_text(file_path, text):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO pdf_text_cache
        VALUES (?, ?)
    """, (file_path, text))

    conn.commit()
    conn.close()


# ==========================================
# CORPORATE TOPIC CACHE
# ==========================================
def get_topic_cache(file_path):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT last_modified FROM corporate_topic_cache WHERE file_path=?
    """, (file_path,))

    row = cursor.fetchone()
    conn.close()

    return row[0] if row else None


def update_topic_cache(file_path, last_modified):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO corporate_topic_cache
        VALUES (?, ?)
    """, (file_path, last_modified))

    conn.commit()
    conn.close()


# ==========================================
# REVIEW HASH (FIXED)
# ==========================================
def review_hash(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ==========================================
# BULK INSERT REVIEWS (FAST)
# ==========================================
def bulk_insert_reviews(cursor, rows):

    cursor.executemany("""
        INSERT OR IGNORE INTO review_sentiments
        (bank_name, year, review_text, review_hash, rating, sentiment_score, sentiment_label)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)


# ==========================================
# FILTER NEW REVIEWS (FIXED)
# ==========================================
def filter_new_reviews(cursor, items):

    new_items = []

    for item in items:

        h = review_hash(item["text"])

        cursor.execute("""
            SELECT 1 FROM review_sentiments
            WHERE review_hash=? LIMIT 1
        """, (h,))

        if cursor.fetchone() is None:
            item["hash"] = h
            new_items.append(item)

    return new_items


# ==========================================
# EMBEDDINGS
# ==========================================
def get_embedding(text):

    conn = get_connection()
    cursor = conn.cursor()

    text_hash = hashlib.md5(text.encode()).hexdigest()

    cursor.execute("""
        SELECT embedding FROM embedding_cache
        WHERE text_hash=?
    """, (text_hash,))

    row = cursor.fetchone()
    conn.close()

    return np.frombuffer(row[0], dtype=np.float32) if row else None


def save_embedding(text, embedding):

    conn = get_connection()
    cursor = conn.cursor()

    text_hash = hashlib.md5(text.encode()).hexdigest()

    cursor.execute("""
        INSERT OR IGNORE INTO embedding_cache
        VALUES (NULL, ?, ?, CURRENT_TIMESTAMP)
    """, (text_hash, embedding.astype(np.float32).tobytes()))

    conn.commit()
    conn.close()


# ==========================================
# CORPORATE TOPIC SENTIMENT
# ==========================================
def save_corporate_topic_sentiment(bank_name, year, topic_scores):

    conn = get_connection()
    cursor = conn.cursor()

    for topic, score in topic_scores.items():

        cursor.execute("""
            INSERT OR REPLACE INTO corporate_topic_sentiment
            VALUES (?, ?, ?, ?)
        """, (bank_name, year, topic, score))

    conn.commit()
    conn.close()

# ==========================================
# BANK MANAGEMENT (RESTORED)
# ==========================================
def register_bank(bank_name):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO banks (bank_name)
        VALUES (?)
    """, (bank_name,))

    conn.commit()
    conn.close()


# ==========================================
# GET REGISTERED BANKS
# ==========================================
def get_registered_banks():

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT bank_name FROM banks")

    banks = [row[0] for row in cursor.fetchall()]

    conn.close()

    return banks