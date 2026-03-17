import os
import re

from scripts.corporate_topic_sentiment import CorporateTopicSentiment
from scripts.transformation_correlation import extract_text_from_pdf
from scripts.db_cache import (
    save_corporate_topic_sentiment,
    get_cached_pdf_text,
    save_pdf_text
)

import sqlite3

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


# ==========================================
# EXTRACT YEAR
# ==========================================

def extract_year(path):

    match = re.search(r"20\d{2}", path)

    if match:
        return int(match.group())

    return None


# ==========================================
# CACHE CHECK
# ==========================================

def is_pdf_processed(cursor, file_path, last_modified):

    cursor.execute("""
        SELECT last_modified
        FROM corporate_topic_cache
        WHERE file_path=?
    """, (file_path,))

    row = cursor.fetchone()

    if row is None:
        return False

    return row[0] == last_modified


def update_topic_cache(cursor, file_path, last_modified):

    cursor.execute("""
        INSERT OR REPLACE INTO corporate_topic_cache
        (file_path, last_modified)
        VALUES (?, ?)
    """, (file_path, last_modified))


# ==========================================
# MAIN
# ==========================================

def main():

    analyzer = CorporateTopicSentiment()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for bank in os.listdir(BASE_CORP_PATH):

        bank_folder = os.path.join(BASE_CORP_PATH, bank)

        if not os.path.isdir(bank_folder):
            continue

        print(f"\n🏦 Processing Corporate Topics for {bank}")

        for root, _, files in os.walk(bank_folder):

            for file in files:

                if not file.endswith(".pdf"):
                    continue

                path = os.path.join(root, file)

                year = extract_year(path)

                if not year:
                    print("⚠ Skipping (no year):", file)
                    continue

                last_modified = os.path.getmtime(path)

                # 🔥 SKIP IF ALREADY PROCESSED
                if is_pdf_processed(cursor, path, last_modified):
                    print("✔ Skipping (cached):", file)
                    continue

                print("📄 Processing:", file)

                # 🔥 USE TEXT CACHE
                text = get_cached_pdf_text(path)

                if not text:
                    text = extract_text_from_pdf(path)
                    save_pdf_text(path, text)

                if not text:
                    continue

                # 🔥 OPTIONAL SPEED BOOST
                text = text[:5000]

                topic_scores = analyzer.analyze(text)

                save_corporate_topic_sentiment(bank, year, topic_scores)

                update_topic_cache(cursor, path, last_modified)

                conn.commit()

                print(f"✔ {bank} {year} topics saved")

    conn.close()

    print("\n✅ Corporate Topic Pipeline Complete")


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":
    main()