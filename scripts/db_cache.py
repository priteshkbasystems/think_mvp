import os
import sqlite3

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