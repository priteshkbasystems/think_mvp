import os
import random
import re
import sqlite3
import hashlib
import numpy as np
from decimal import Decimal, InvalidOperation

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


def normalize_bank_display_name(name):
    """e.g. Bangkok_Bank / folder names -> Bangkok Bank (spaces, trimmed)."""
    if name is None:
        return None
    s = str(name).replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


# Min Euclidean distance in RGB (0–255) between any two bank colors.
_MIN_BANK_COLOR_DISTANCE = 78.0
_MAX_COLOR_PICK_ATTEMPTS = 800


def _parse_hex_color(value):
    if not value or not isinstance(value, str):
        return None
    m = re.match(r"^#?([0-9a-fA-F]{6})$", value.strip())
    if not m:
        return None
    h = m.group(1)
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _is_black_white_or_gray_shade(r, g, b):
    """Reject near-black, near-white, and low-chroma (gray) shades."""
    mx, mn = max(r, g, b), min(r, g, b)
    chroma = mx - mn
    # sRGB relative luminance (0..1)
    lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0
    if lum <= 0.10:
        return True
    if lum >= 0.93:
        return True
    if mx <= 42 and mn <= 42:
        return True
    if mn >= 218 and mx >= 218:
        return True
    if chroma < 52:
        return True
    return False


def _rgb_distance(a, b):
    return float(
        ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2) ** 0.5
    )


def _pick_distinct_hex(existing_rgbs):
    """Return a new #RRGGBB color far from existing_rgbs and not B/W/gray."""
    for _ in range(_MAX_COLOR_PICK_ATTEMPTS):
        rgb = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        if _is_black_white_or_gray_shade(*rgb):
            continue
        if all(_rgb_distance(rgb, e) >= _MIN_BANK_COLOR_DISTANCE for e in existing_rgbs):
            return f"#{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}"
    # Relax distance slightly if palette is crowded
    for _ in range(_MAX_COLOR_PICK_ATTEMPTS):
        rgb = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        if _is_black_white_or_gray_shade(*rgb):
            continue
        if all(_rgb_distance(rgb, e) >= _MIN_BANK_COLOR_DISTANCE * 0.65 for e in existing_rgbs):
            return f"#{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}"
    rgb = (random.randint(60, 220), random.randint(60, 220), random.randint(60, 220))
    while _is_black_white_or_gray_shade(*rgb):
        rgb = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
    return f"#{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}"


def _ensure_banks_color_column(cursor):
    cursor.execute("PRAGMA table_info(banks)")
    cols = {r[1] for r in cursor.fetchall()}
    if "color" not in cols:
        cursor.execute("ALTER TABLE banks ADD COLUMN color TEXT")


def _collect_existing_bank_colors(cursor, exclude_bank_name=None):
    cursor.execute(
        "SELECT bank_name, color FROM banks WHERE color IS NOT NULL AND TRIM(color) != ''"
    )
    existing_rgbs = []
    for bn, col in cursor.fetchall():
        if exclude_bank_name and bn == exclude_bank_name:
            continue
        parsed = _parse_hex_color(col)
        if parsed:
            existing_rgbs.append(parsed)
    return existing_rgbs


def assign_bank_color_if_missing(cursor, bank_name):
    """Assign validated distinct hex color for one bank if color is NULL/empty."""
    bank_name = normalize_bank_display_name(bank_name)
    if not bank_name:
        return
    cursor.execute(
        "SELECT color FROM banks WHERE bank_name=?",
        (bank_name,),
    )
    row = cursor.fetchone()
    if not row:
        return
    cur = row[0]
    if cur and str(cur).strip():
        return
    existing = _collect_existing_bank_colors(cursor, exclude_bank_name=bank_name)
    hex_color = _pick_distinct_hex(existing)
    cursor.execute(
        "UPDATE banks SET color=? WHERE bank_name=?",
        (hex_color, bank_name),
    )


def backfill_all_bank_colors(cursor):
    """Assign colors for every bank missing one, ordered by bank_id for stability."""
    cursor.execute(
        "SELECT bank_name FROM banks WHERE color IS NULL OR TRIM(color) = '' ORDER BY bank_id, bank_name"
    )
    names = [r[0] for r in cursor.fetchall()]
    for name in names:
        assign_bank_color_if_missing(cursor, name)


def ensure_bank_registered_with_color(bank_name):
    """INSERT bank if missing and assign color when needed (single connection)."""
    bank_name = normalize_bank_display_name(bank_name)
    if not bank_name:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO banks (bank_name) VALUES (?)", (bank_name,))
    assign_bank_color_if_missing(cursor, bank_name)
    conn.commit()
    conn.close()


def _migrate_financial_metrics_to_text_if_needed(cursor):
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='financial_metrics'"
    )
    if not cursor.fetchone():
        return
    cursor.execute("PRAGMA table_info(financial_metrics)")
    rows = cursor.fetchall()
    if not rows:
        return
    col_types = {row[1]: row[2].upper() for row in rows}
    if col_types.get("revenue") == "TEXT":
        return

    def to_text(v):
        if v is None:
            return None
        try:
            d = Decimal(str(v))
        except InvalidOperation:
            return None
        if d == d.to_integral():
            return str(int(d))
        return format(d, "f").rstrip("0").rstrip(".") or "0"

    cursor.execute(
        """
        SELECT bank_name, year, revenue, net_profit, operating_income, total_assets, roe
        FROM financial_metrics
        """
    )
    old_rows = cursor.fetchall()
    cursor.execute("DROP TABLE financial_metrics")
    cursor.execute(
        """
        CREATE TABLE financial_metrics (
            bank_name TEXT,
            year INTEGER,
            revenue TEXT,
            net_profit TEXT,
            operating_income TEXT,
            total_assets TEXT,
            roe TEXT,
            PRIMARY KEY(bank_name, year)
        )
        """
    )
    for (
        bank_name,
        year,
        revenue,
        net_profit,
        operating_income,
        total_assets,
        roe,
    ) in old_rows:
        cursor.execute(
            """
            INSERT INTO financial_metrics
            (bank_name, year, revenue, net_profit, operating_income, total_assets, roe)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bank_name,
                year,
                to_text(revenue),
                to_text(net_profit),
                to_text(operating_income),
                to_text(total_assets),
                to_text(roe),
            ),
        )


# ==========================================
# CONNECTION
# ==========================================
def get_connection():
    return sqlite3.connect(DB_PATH)


def _ensure_bank_id_in_banks(cursor):
    cursor.execute("PRAGMA table_info(banks)")
    cols = {r[1] for r in cursor.fetchall()}
    if "bank_id" not in cols:
        cursor.execute("ALTER TABLE banks ADD COLUMN bank_id INTEGER")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_banks_bank_id ON banks(bank_id)")
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_banks_autobankid
        AFTER INSERT ON banks
        WHEN NEW.bank_id IS NULL
        BEGIN
            UPDATE banks
            SET bank_id = (
                SELECT COALESCE(MAX(bank_id), 0) + 1
                FROM banks
                WHERE rowid <> NEW.rowid
            )
            WHERE rowid = NEW.rowid;
        END;
        """
    )
    # Backfill missing bank_id for existing rows
    cursor.execute("SELECT rowid FROM banks WHERE bank_id IS NULL ORDER BY rowid")
    for (rid,) in cursor.fetchall():
        cursor.execute(
            """
            UPDATE banks
            SET bank_id = (SELECT COALESCE(MAX(bank_id), 0) + 1 FROM banks)
            WHERE rowid = ?
            """,
            (rid,),
        )


def _migrate_bank_id_across_tables(cursor):
    _ensure_bank_id_in_banks(cursor)

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [r[0] for r in cursor.fetchall()]

    bank_tables = []
    for table in all_tables:
        if table.startswith("sqlite_"):
            continue
        cursor.execute(f"PRAGMA table_info({table})")
        cols = {r[1] for r in cursor.fetchall()}
        if "bank_name" in cols:
            bank_tables.append((table, cols))

    # Add bank_id + backfill + auto-fill trigger for each table
    for table, cols in bank_tables:
        if table != "banks" and "bank_id" not in cols:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN bank_id INTEGER")

        if table != "banks":
            cursor.execute(
                f"""
                UPDATE {table}
                SET bank_id = (
                    SELECT b.bank_id
                    FROM banks b
                    WHERE b.bank_name = {table}.bank_name
                )
                WHERE bank_name IS NOT NULL
                  AND (bank_id IS NULL)
                """
            )
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_bank_id ON {table}(bank_id)")

            cursor.execute(f"DROP TRIGGER IF EXISTS trg_{table}_autobankid")
            cursor.execute(
                f"""
                CREATE TRIGGER trg_{table}_autobankid
                AFTER INSERT ON {table}
                WHEN NEW.bank_id IS NULL AND NEW.bank_name IS NOT NULL
                BEGIN
                    UPDATE {table}
                    SET bank_id = (SELECT bank_id FROM banks WHERE bank_name = NEW.bank_name)
                    WHERE rowid = NEW.rowid;
                END;
                """
            )


def _drop_legacy_tables(cursor):
    for name in (
        "sentiment_taxonomy",
        "conversation_sentiment_flow",
        "human_labels",
    ):
        cursor.execute(f"DROP TABLE IF EXISTS {name}")


# ==========================================
# INIT DB (FULL PIPELINE SAFE)
# ==========================================
def init_db():

    conn = get_connection()
    cursor = conn.cursor()

    _drop_legacy_tables(cursor)

    # ------------------------------
    # CORE TABLES
    # ------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS banks (
        bank_name TEXT PRIMARY KEY,
        color TEXT
    )
    """)
    _ensure_banks_color_column(cursor)

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
        revenue TEXT,
        net_profit TEXT,
        operating_income TEXT,
        total_assets TEXT,
        roe TEXT,
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transformation_impact_scores (
        bank_id INTEGER PRIMARY KEY,
        bank_name TEXT,
        tis_score REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transformation_performance_index (
        bank_id INTEGER PRIMARY KEY,
        bank_name TEXT,
        score REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transformation_lag_results (
        bank_id INTEGER PRIMARY KEY,
        bank_name TEXT,
        lag_years INTEGER,
        correlation REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    CREATE TABLE IF NOT EXISTS topic_sentiment_correlation (
        bank_id INTEGER PRIMARY KEY,
        bank_name TEXT,
        correlation REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS journey_sentiment (
        stage TEXT PRIMARY KEY,
        sentiment REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS success_factors (
        bank_id INTEGER,
        bank_name TEXT,
        topic_id INTEGER,
        keywords TEXT,
        sentiment REAL,
        volume INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(bank_id, topic_id)
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_sentence_sentiment (
        bank_name TEXT,
        year INTEGER,
        file_path TEXT,
        sentence_index INTEGER,
        page_number INTEGER,
        sentence_text TEXT,
        sentiment_label TEXT,
        sentiment_score REAL,
        signed_score REAL,
        utterance_kind TEXT,
        topic TEXT,
        label TEXT,
        PRIMARY KEY (bank_name, year, file_path, sentence_index)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_page_sentiment (
        bank_name TEXT,
        year INTEGER,
        file_path TEXT,
        page_number INTEGER,
        mean_signed REAL,
        sentence_count INTEGER,
        label TEXT,
        PRIMARY KEY (bank_name, year, file_path, page_number)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS corporate_document_sentiment_rollup (
        bank_name TEXT,
        year INTEGER,
        file_path TEXT,
        doc_mean_signed REAL,
        label TEXT,
        PRIMARY KEY (bank_name, year, file_path)
    )
    """)

    _migrate_financial_metrics_to_text_if_needed(cursor)
    _migrate_bank_id_across_tables(cursor)
    backfill_all_bank_colors(cursor)
    conn.commit()
    conn.close()


def delete_corporate_hierarchy_for_file(conn, bank_name, year, file_path):
    cursor = conn.cursor()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
    row = cursor.fetchone()
    bank_id = row[0] if row else None
    cursor.execute(
        """
        DELETE FROM corporate_sentence_sentiment
        WHERE bank_id=? AND year=? AND file_path=?
        """,
        (bank_id, year, file_path),
    )
    cursor.execute(
        """
        DELETE FROM corporate_page_sentiment
        WHERE bank_id=? AND year=? AND file_path=?
        """,
        (bank_id, year, file_path),
    )
    cursor.execute(
        """
        DELETE FROM corporate_document_sentiment_rollup
        WHERE bank_id=? AND year=? AND file_path=?
        """,
        (bank_id, year, file_path),
    )


def save_corporate_hierarchy_sentiment(conn, bank_name, year, file_path, result):
    """result: dict with keys sentences, pages, document from CorporateSentimentAnalyzer.analyze_pages."""
    delete_corporate_hierarchy_for_file(conn, bank_name, year, file_path)
    cursor = conn.cursor()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
    row = cursor.fetchone()
    if not row:
        return
    bank_id = row[0]

    for row in result["sentences"]:
        cursor.execute(
            """
            INSERT INTO corporate_sentence_sentiment
            (bank_id, bank_name, year, file_path, sentence_index, page_number, sentence_text,
             sentiment_label, sentiment_score, signed_score, utterance_kind, topic, label)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bank_id,
                bank_name,
                year,
                file_path,
                row["sentence_index"],
                row["page_number"],
                row["sentence"],
                row["sentiment_label"],
                row["sentiment_score"],
                row["signed_score"],
                row["utterance_kind"],
                row["topic"],
                row.get("label"),
            ),
        )

    for prow in result["pages"]:
        cursor.execute(
            """
            INSERT INTO corporate_page_sentiment
            (bank_id, bank_name, year, file_path, page_number, mean_signed, sentence_count, label)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bank_id,
                bank_name,
                year,
                file_path,
                prow["page_number"],
                prow["mean_signed"],
                prow["sentence_count"],
                prow.get("label"),
            ),
        )

    doc = result["document"]
    cursor.execute(
        """
        INSERT INTO corporate_document_sentiment_rollup
        (bank_id, bank_name, year, file_path, doc_mean_signed, label)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            bank_id,
            bank_name,
            year,
            file_path,
            doc["doc_mean_signed"],
            doc.get("label"),
        ),
    )


# ==========================================
# BANK FUNCTIONS
# ==========================================
def register_bank(bank_name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def get_bank_id(bank_name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


# ==========================================
# STOCK
# ==========================================
def save_stock_return(bank, year, value):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return
    bank_id = row[0]
    cursor.execute("""
    INSERT OR REPLACE INTO stock_returns (bank_id, bank_name, year, return)
    VALUES (?, ?, ?, ?)
    """, (bank_id, bank, year, value))
    conn.commit()
    conn.close()


# ==========================================
# SENTIMENT
# ==========================================
def save_sentiment_score(bank, year, sentiment, contradiction):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return
    bank_id = row[0]
    cursor.execute("""
    INSERT OR REPLACE INTO sentiment_scores (bank_id, bank_name, year, sentiment, contradiction_ratio)
    VALUES (?, ?, ?, ?, ?)
    """, (bank_id, bank, year, sentiment, contradiction))
    conn.commit()
    conn.close()


def save_review_sentiment(bank, year, text, rating, score, label):

    conn = get_connection()
    cursor = conn.cursor()

    h = hashlib.md5(text.encode("utf-8")).hexdigest()
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return
    bank_id = row[0]

    cursor.execute("""
    INSERT OR IGNORE INTO review_sentiments
    (bank_id, bank_name, year, review_text, review_hash, rating, sentiment_score, sentiment_label)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (bank_id, bank, year, text, h, rating, score, label))

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

    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return
    bank_id = row[0]
    for topic, score in topic_scores.items():

        cursor.execute("""
        INSERT OR REPLACE INTO corporate_topic_sentiment
        (bank_id, bank_name, year, topic, sentiment)
        VALUES (?, ?, ?, ?, ?)
        """, (bank_id, bank_name, year, topic, score))

    conn.commit()
    conn.close()