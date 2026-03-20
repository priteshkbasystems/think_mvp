import os
import sqlite3
import hashlib
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


# ==========================================
# CONNECTION
# ==========================================
def get_connection():
    return sqlite3.connect(DB_PATH)


# ==========================================
# INIT DB (FULL PIPELINE SAFE)
# ==========================================
def init_db():

    conn = get_connection()
    cursor = conn.cursor()

    # ------------------------------
    # CORE TABLES
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS banks (
        bank_name TEXT PRIMARY KEY
    )
    """)

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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS financial_statement_sheets (
        bank_name TEXT,
        year INTEGER,
        file_path TEXT,
        sheet_name TEXT,
        payload_json TEXT,
        PRIMARY KEY(bank_name, year, file_path, sheet_name)
    )
    """)

    # ------------------------------
    # REVIEW SENTIMENT
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
        topic_id INTEGER,
        review_source TEXT
    )
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_review_hash
    ON review_sentiments(review_hash)
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_scores (
        bank_name TEXT,
        year INTEGER,
        sentiment REAL,
        contradiction_ratio REAL,
        PRIMARY KEY (bank_name, year)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_predictions (
        bank_name TEXT,
        year INTEGER,
        predicted_sentiment REAL,
        PRIMARY KEY(bank_name, year)
    )
    """)

    # ------------------------------
    # TOPICS
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_topic_sentiment (
        bank_name TEXT,
        year INTEGER,
        topic TEXT,
        sentiment REAL,
        PRIMARY KEY(bank_name, year, topic)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_topic_cache (
        file_path TEXT PRIMARY KEY,
        last_modified REAL
    )
    """)

    # ------------------------------
    # TRANSFORMATION
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pdf_cache (
        file_path TEXT PRIMARY KEY,
        last_modified REAL,
        year INTEGER,
        score REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pdf_text_cache (
        file_path TEXT PRIMARY KEY,
        text TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS embedding_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text_hash TEXT UNIQUE,
        embedding BLOB
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transformation_competencies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name TEXT,
        year INTEGER,
        competency TEXT,
        score REAL
    )
    """)

    # ------------------------------
    # NARRATIVE
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_scores (
        bank_name TEXT,
        year INTEGER,
        score REAL,
        PRIMARY KEY(bank_name, year)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_sentiment_correlation (
        bank_name TEXT PRIMARY KEY,
        correlation REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_lag (
        bank_name TEXT PRIMARY KEY,
        lag_months INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS narrative_highlights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name TEXT,
        year INTEGER,
        highlight TEXT
    )
    """)

    # ------------------------------
    # ADVANCED ANALYTICS
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_concordance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name TEXT,
        review_source TEXT,
        avg_sentiment REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_taxonomy (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name TEXT,
        year INTEGER,
        review_text TEXT,
        emotion TEXT,
        category TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS conversation_sentiment_flow (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id TEXT,
        step INTEGER,
        message TEXT,
        sentiment REAL
    )
    """)

    # ------------------------------
    # PIPELINE CONTROL
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        step_name TEXT PRIMARY KEY,
        last_run TIMESTAMP,
        status TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS step_progress (
        step_name TEXT,
        bank_name TEXT,
        year INTEGER,
        last_processed_index INTEGER,
        PRIMARY KEY(step_name, bank_name, year)
    )
    """)
    # -----------------------------------------
    # CORPORATE SENTIMENT
    # -----------------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_sentiment (
        bank_name TEXT,
        year INTEGER,
        sentiment REAL,
        PRIMARY KEY (bank_name, year)
    )
    """)

    # ------------------------------
    # FINANCIAL FULL
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS financial_full (
    bank_name TEXT,
    year INTEGER,
    period_type TEXT,

    -- INCOME
    interest_income REAL,
    net_interest_income REAL,
    fee_income REAL,
    total_operating_income REAL,
    operating_expenses REAL,
    credit_loss REAL,
    net_profit REAL,

    -- BALANCE SHEET
    total_assets REAL,
    total_liabilities REAL,
    total_equity REAL,
    loans REAL,
    deposits REAL,

    -- CASH FLOW
    operating_cashflow REAL,
    investing_cashflow REAL,
    financing_cashflow REAL,

    -- RATIOS
    roe REAL,
    loan_to_deposit REAL,
    cost_to_income REAL,
    car REAL,
    tier1_ratio REAL,
    cet1_ratio REAL,

    PRIMARY KEY (bank_name, year, period_type)
    )
    """)
    # ==========================================
    # HUMAN FEEDBACK LOOP
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS human_labels(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_text TEXT,
    ai_label TEXT,
    human_label TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()


# ==========================================
# BANK FUNCTIONS
# ==========================================
def register_bank(bank_name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO banks VALUES (?)", (bank_name,))
    conn.commit()
    conn.close()


# ==========================================
# STOCK
# ==========================================
def save_stock_return(bank, year, value):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR REPLACE INTO stock_returns VALUES (?, ?, ?)
    """, (bank, year, value))
    conn.commit()
    conn.close()


# ==========================================
# SENTIMENT
# ==========================================
def save_sentiment_score(bank, year, sentiment, contradiction):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR REPLACE INTO sentiment_scores VALUES (?, ?, ?, ?)
    """, (bank, year, sentiment, contradiction))
    conn.commit()
    conn.close()


def save_review_sentiment(bank, year, text, rating, score, label):

    conn = get_connection()
    cursor = conn.cursor()

    h = hashlib.md5(text.encode("utf-8")).hexdigest()

    cursor.execute("""
    INSERT OR IGNORE INTO review_sentiments
    (bank_name, year, review_text, review_hash, rating, sentiment_score, sentiment_label)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (bank, year, text, h, rating, score, label))

    conn.commit()
    conn.close()


# ==========================================
# TAXONOMY (FIXED YOUR ERROR)
# ==========================================
def save_sentiment_taxonomy(bank, year, text, emotion, category):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO sentiment_taxonomy
    (bank_name, year, review_text, emotion, category)
    VALUES (?, ?, ?, ?, ?)
    """, (bank, year, text, emotion, category))

    conn.commit()
    conn.close()


# ==========================================
# EMBEDDINGS
# ==========================================
def get_embedding(text):

    conn = get_connection()
    cursor = conn.cursor()

    h = hashlib.md5(text.encode()).hexdigest()

    cursor.execute("SELECT embedding FROM embedding_cache WHERE text_hash=?", (h,))
    row = cursor.fetchone()
    conn.close()

    return np.frombuffer(row[0], dtype=np.float32) if row else None


def save_embedding(text, embedding):

    conn = get_connection()
    cursor = conn.cursor()

    h = hashlib.md5(text.encode()).hexdigest()

    cursor.execute("""
    INSERT OR IGNORE INTO embedding_cache (text_hash, embedding)
    VALUES (?, ?)
    """, (h, embedding.astype(np.float32).tobytes()))

    conn.commit()
    conn.close()


# ==========================================
# PDF CACHE
# ==========================================
def get_cached_score(path):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT last_modified, year, score FROM pdf_cache WHERE file_path=?", (path,))
    row = cursor.fetchone()
    conn.close()
    return row


def update_cache(path, last_modified, year, score):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR REPLACE INTO pdf_cache VALUES (?, ?, ?, ?)
    """, (path, last_modified, year, score))
    conn.commit()
    conn.close()


def get_cached_pdf_text(path):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT text FROM pdf_text_cache WHERE file_path=?", (path,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def save_pdf_text(path, text):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO pdf_text_cache VALUES (?, ?)", (path, text))
    conn.commit()
    conn.close()

# ==========================================
# FILE MODIFIED TIME (FIX)
# ==========================================
def get_file_modified_time(path):
    return os.path.getmtime(path)


# ==========================================
# CORPORATE TOPIC CACHE (FIX)
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
# CORPORATE TOPIC SENTIMENT (FIX)
# ==========================================
def save_corporate_topic_sentiment(bank_name, year, topic_scores):

    conn = get_connection()
    cursor = conn.cursor()

    for topic, score in topic_scores.items():

        cursor.execute("""
        INSERT OR REPLACE INTO corporate_topic_sentiment
        (bank_name, year, topic, sentiment)
        VALUES (?, ?, ?, ?)
        """, (bank_name, year, topic, score))

    conn.commit()
    conn.close()