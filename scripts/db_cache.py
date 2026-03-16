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
    # Human labels
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS human_labels(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_text TEXT,
    ai_label TEXT,
    human_label TEXT)
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
    review_hash TEXT UNIQUE,
    rating REAL,
    sentiment_score REAL,
    sentiment_label TEXT,
    review_source TEXT
    )
    """)

    # ==========================================
    # INDEX FOR REVIEW HASH
    # ==========================================
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_review_hash
    ON review_sentiments(review_hash)
    """)
    # ==========================================
    # COMPLAINT TOPICS
    # ==========================================
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
    # ==========================================
    # EMBEDDING CACHE
    # ==========================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS embedding_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text_hash TEXT UNIQUE,
        embedding BLOB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # ==========================================
    # TRANSFORMATION COMPETENCIES
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transformation_competencies(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_name TEXT,
    year INTEGER,
    competency TEXT,
    score REAL)
    """)
    # ==========================================
    # CONVERSATION SENTIMENT FLOW
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS conversation_sentiment_flow(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT,
    step INTEGER,
    message TEXT,
    sentiment REAL)
    """)
    # ==========================================
    # CORPORATE SENTIMENT
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_sentiment(
    bank_name TEXT,
    year INTEGER,
    sentiment REAL)
    """)
    # ==========================================
    # DASHBOARD NARRATIVE SCORE
    # ==========================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_scores (
    bank_name TEXT,
    year INTEGER,
    score REAL,
    PRIMARY KEY(bank_name, year)
    )
    """)

    # ==========================================
    # CORRELATION BETWEEN NARRATIVE AND SENTIMENT
    # ==========================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_sentiment_correlation (
    bank_name TEXT PRIMARY KEY,
    correlation REAL
    )
    """)

    # ==========================================
    # NARRATIVE → SENTIMENT LAG
    # ==========================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_lag (
    bank_name TEXT PRIMARY KEY,
    lag_months INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_topic_sentiment (
    bank_name TEXT,
    year INTEGER,
    topic TEXT,
    sentiment REAL,
    PRIMARY KEY(bank_name, year, topic)
    )
    """)
    # ==========================================
    # SENTIMENT PREDICTION
    # ==========================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_predictions (
    bank_name TEXT,
    year INTEGER,
    predicted_sentiment REAL,
    PRIMARY KEY(bank_name, year)
    )
    """)
    # ==========================================
    # SOURCE CONCORDANCE
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_concordance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_name TEXT,
    review_source TEXT,
    avg_sentiment REAL)
    """)
    # ==========================================
    # SENTIMENT TAXONOMY
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_taxonomy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_name TEXT,
    year INTEGER,
    review_text TEXT,
    emotion TEXT,
    category TEXT)
    """)
    # ==========================================
    # PIPELINE RUNS
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
    step_name TEXT PRIMARY KEY,
    last_run TIMESTAMP,
    status TEXT)
    """)
    # ==========================================
    # NARRATIVE HIGHLIGHTS
    # ==========================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_highlights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_name TEXT,
    year INTEGER,
    highlight TEXT
    )
    """)

    # ==========================================
    # FINANCIAL METRICS
    # ==========================================
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
    # ==========================================
    # STEP PROGRESS
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS step_progress (
    step_name TEXT,
    bank_name TEXT,
    year INTEGER,
    last_processed_index INTEGER,
    PRIMARY KEY(step_name, bank_name, year)
    )
    """)
    # ==========================================
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

    review_hash = hashlib.md5(text.encode("utf-8")).hexdigest()

    cursor.execute("""
        INSERT OR IGNORE INTO review_sentiments
        (bank_name, year, review_text, review_hash, rating, sentiment_score, sentiment_label)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (bank, year, text, review_hash, rating, score, label))

    conn.commit()
    conn.close()
# ==========================================
# FILTER NEW REVIEWS
# ==========================================

def filter_new_reviews(cursor, items):

    new_items = []

    for item in items:

        h = review_hash(item["text"])

        cursor.execute(
            "SELECT 1 FROM review_sentiments WHERE review_hash=? LIMIT 1",
            (h,)
        )

        if cursor.fetchone() is None:
            item["hash"] = h
            new_items.append(item)

    return new_items
# ==========================================
# BULK INSERT REVIEWS
# ==========================================
def bulk_insert_reviews(cursor, rows):

    cursor.executemany(
        """
        INSERT OR IGNORE INTO review_sentiments
        (bank_name, year, review_text, review_hash, rating, sentiment_score, sentiment_label)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows
    )
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

# ==========================================
# CORPORATE TOPIC SENTIMENT
# ==========================================

def save_corporate_topic_sentiment(bank_name, year, topic_scores):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for topic, score in topic_scores.items():

        cursor.execute("""
        INSERT OR REPLACE INTO corporate_topic_sentiment
        (bank_name, year, topic, sentiment)
        VALUES (?, ?, ?, ?)
        """, (bank_name, year, topic, score))

    conn.commit()
    conn.close()

# ==========================================
# SAVE SENTIMENT TAXONOMY
# ==========================================
def save_sentiment_taxonomy(bank_name, year, text, emotion, category):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO sentiment_taxonomy
    (bank_name, year, review_text, emotion, category)
    VALUES (?, ?, ?, ?, ?)
    """, (bank_name, year, text, emotion, category))

    conn.commit()
    conn.close()