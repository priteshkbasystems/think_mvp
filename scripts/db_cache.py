import os
import sqlite3
import hashlib
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


# ==========================================
# DATABASE INITIALIZATION
# ==========================================

def init_db():

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ------------------------------
    # PDF transformation cache
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
    # Banks table
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS banks (
            bank_name TEXT PRIMARY KEY
        )
    """)

    # ------------------------------
    # Sentiment results
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
    # Stock yearly returns
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_returns (
            bank_name TEXT,
            year INTEGER,
            return REAL,
            PRIMARY KEY (bank_name, year)
        )
    """)


    # ------------------------------
    # Review sentiments
    # ------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS review_sentiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT,
            year INTEGER,
            review_text TEXT,
            rating REAL,
            sentiment_score REAL,
            sentiment_label TEXT
        )
    """)
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS embedding_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text_hash TEXT UNIQUE,
        embedding BLOB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
# PDF CACHE FUNCTIONS
# ==========================================

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


# ==========================================
# BANK MANAGEMENT
# ==========================================

def register_bank(bank_name):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO banks (bank_name)
        VALUES (?)
    """, (bank_name,))

    conn.commit()
    conn.close()


def get_registered_banks():

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT bank_name FROM banks")

    banks = [row[0] for row in cursor.fetchall()]

    conn.close()

    return banks


# ==========================================
# SENTIMENT CACHE
# ==========================================

def save_sentiment(bank_name, year, sentiment, contradiction_ratio):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO sentiment_scores
        (bank_name, year, sentiment, contradiction_ratio)
        VALUES (?, ?, ?, ?)
    """, (bank_name, year, sentiment, contradiction_ratio))

    conn.commit()
    conn.close()


def get_sentiment(bank_name):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, sentiment, contradiction_ratio
        FROM sentiment_scores
        WHERE bank_name=?
    """, (bank_name,))

    rows = cursor.fetchall()

    conn.close()

    return rows


# ==========================================
# STOCK RETURN CACHE
# ==========================================

def save_stock_return(bank_name, year, value):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO stock_returns
        (bank_name, year, return)
        VALUES (?, ?, ?)
    """, (bank_name, year, value))

    conn.commit()
    conn.close()


def get_stock_returns(bank_name):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, return
        FROM stock_returns
        WHERE bank_name=?
    """ , (bank_name,))

    rows = cursor.fetchall()

    conn.close()

    return rows

def save_sentiment_score(bank, year, sentiment, contradiction):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO sentiment_scores
        (bank_name, year, sentiment, contradiction_ratio)
        VALUES (?, ?, ?, ?)
    """, (bank, year, sentiment, contradiction))

    conn.commit()
    conn.close()

# ==========================================
# REVIEW SENTIMENT CACHE
# ==========================================

def save_review_sentiment(bank, year, text, rating, score, label):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO review_sentiments
        (bank_name, year, review_text, rating, sentiment_score, sentiment_label)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (bank, year, text, rating, score, label))

    conn.commit()
    conn.close()

# ==========================================
# COMPLAINT TOPICS CACHE
# ==========================================

def save_complaint_topics(bank_name, topics):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for topic_id, keywords in topics.items():

        cursor.execute("""
            INSERT INTO complaint_topics
            (bank_name, topic_id, keywords)
            VALUES (?, ?, ?)
        """, (bank_name, topic_id, ",".join(keywords)))

    conn.commit()
    conn.close()

# ==========================================
# EMBEDDING CACHE
# ==========================================

def get_embedding(text):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    text_hash = hashlib.md5(text.encode()).hexdigest()

    cursor.execute(
        "SELECT embedding FROM embedding_cache WHERE text_hash=?",
        (text_hash,)
    )

    row = cursor.fetchone()

    conn.close()

    if row:
        return np.frombuffer(row[0], dtype=np.float32)

    return None

# ==========================================
# SAVE EMBEDDING
# ==========================================

def save_embedding(text, embedding):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    text_hash = hashlib.md5(text.encode()).hexdigest()

    cursor.execute(
        """
        INSERT OR IGNORE INTO embedding_cache
        (text_hash, embedding)
        VALUES (?, ?)
        """,
        (text_hash, embedding.astype(np.float32).tobytes())
    )

    conn.commit()
    conn.close()