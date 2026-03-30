import os
import json
import sqlite3
import pandas as pd
from collections import defaultdict
import hashlib
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

from models.sentiment_model import SentimentModel
from scripts.utils.sentiment_utils import sentiment_label
from scripts.progress_tracker import ProgressTracker
from scripts.db_cache import register_bank, get_bank_id


BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
JSON_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"

TEXT_WEIGHT = 0.7
RATING_WEIGHT = 0.3
CONTRADICTION_THRESHOLD = 0.8

BATCH_SIZE = 256
STEP_NAME = "STEP 2 — SENTIMENT TREND"


# --------------------------------------------------
# Generate review hash
# --------------------------------------------------

def review_hash(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# --------------------------------------------------
# Normalize rating
# --------------------------------------------------

def normalize_rating(star_rating):
    return (star_rating - 3) / 2


# --------------------------------------------------
# Fuse sentiment
# --------------------------------------------------

def fuse_sentiment(text_score, rating):

    if rating is None or pd.isna(rating):
        return text_score, False

    normalized_rating = normalize_rating(rating)

    final = TEXT_WEIGHT * text_score + RATING_WEIGHT * normalized_rating

    contradiction = abs(text_score - normalized_rating) > CONTRADICTION_THRESHOLD

    return final, contradiction


# --------------------------------------------------
# Discover review folders
# --------------------------------------------------

def discover_review_folders(base_path):

    banks = {}

    if not os.path.exists(base_path):
        print("⚠ Base path not found:", base_path)
        return banks

    for bank_folder in os.listdir(base_path):

        bank_path = os.path.join(base_path, bank_folder)

        if not os.path.isdir(bank_path):
            continue

        reviews_path = os.path.join(bank_path, "Reviews")

        if os.path.exists(reviews_path):

            display_name = bank_folder.replace("_", " ")

            banks[display_name] = reviews_path

    return banks


# --------------------------------------------------
# Load reviews (ALL Excel sheets)
# --------------------------------------------------

def load_reviews(folder):

    data = []

    for file in os.listdir(folder):

        if not file.endswith(".xlsx"):
            continue

        path = os.path.join(folder, file)

        try:
            xls = pd.ExcelFile(path)
        except:
            continue

        for sheet in xls.sheet_names:

            try:
                df = pd.read_excel(xls, sheet_name=sheet)
            except:
                continue

            if "Date" not in df.columns or "review" not in df.columns:
                continue

            df["Date"] = pd.to_datetime(
                df["Date"],
                errors="coerce",
                dayfirst=True
            )

            df = df.dropna(subset=["Date"])

            df["review"] = df["review"].astype(str)

            if "Rating" in df.columns:
                df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
            else:
                df["Rating"] = None

            for _, row in df.iterrows():

                text = row["review"]

                data.append({
                    "year": int(row["Date"].year),
                    "text": text,
                    "rating": row["Rating"],
                    "hash": review_hash(text),
                    "source": f"{file}::{sheet}"
                })

    return data


# --------------------------------------------------
# Detect trend
# --------------------------------------------------

def detect_trend(sentiments):

    years = sorted(sentiments.keys())

    if len(years) < 2:
        return "Insufficient Data"

    change = sentiments[years[-1]] - sentiments[years[0]]

    if change > 0.02:
        return "Improving"

    if change < -0.02:
        return "Declining"

    return "Stable"


# --------------------------------------------------
# Bulk insert reviews
# --------------------------------------------------

def bulk_insert_reviews(cursor, rows):

    cursor.executemany(
        """
        INSERT OR IGNORE INTO review_sentiments
        (bank_id, bank_name, year, review_text, review_hash, rating, sentiment_score, sentiment_label, review_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows
    )


# --------------------------------------------------
# Backfill review source for existing rows
# --------------------------------------------------

def backfill_review_sources(cursor, bank_name, items):

    rows = []
    for item in items:
        source = item.get("source")
        h = item.get("hash")
        if source and h:
            rows.append((source, bank_name, h))

    if not rows:
        return

    cursor.executemany(
        """
        UPDATE review_sentiments
        SET review_source = ?
        WHERE bank_name = ?
          AND review_hash = ?
          AND (review_source IS NULL OR TRIM(review_source) = '')
        """,
        rows
    )


# --------------------------------------------------
# Filter already processed reviews
# --------------------------------------------------

def filter_new_reviews(cursor, items):

    new_items = []

    for item in items:

        cursor.execute(
            "SELECT 1 FROM review_sentiments WHERE review_hash=? AND bank_id=? LIMIT 1",
            (item["hash"], item["bank_id"])
        )

        if cursor.fetchone() is None:
            new_items.append(item)

    return new_items


# --------------------------------------------------
# Worker
# --------------------------------------------------

def process_bank(args):

    bank, path = args

    sentiment_model = SentimentModel()
    tracker = ProgressTracker()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    register_bank(bank)
    bank_id = get_bank_id(bank)

    data = load_reviews(path)
    for d in data:
        d["bank_id"] = bank_id

    if len(data) == 0:
        return bank, None

    # Ensure existing rows get review_source populated.
    backfill_review_sources(cursor, bank, data)
    conn.commit()

    year_groups = defaultdict(list)

    for d in data:
        year_groups[d["year"]].append(d)

    year_sentiments = {}
    yearly_contradictions = {}

    print("\n🏦 Processing:", bank)

    for year in sorted(year_groups.keys()):

        items = year_groups[year]

        items = filter_new_reviews(cursor, items)

        if len(items) == 0:
            continue

        fused_scores = []
        contradiction_count = 0

        texts = [i["text"] for i in items]
        ratings = [i["rating"] for i in items]
        hashes = [i["hash"] for i in items]
        sources = [i.get("source") for i in items]
        bank_ids = [i["bank_id"] for i in items]

        for i in range(0, len(texts), BATCH_SIZE):

            batch_texts = texts[i:i+BATCH_SIZE]
            batch_ratings = ratings[i:i+BATCH_SIZE]
            batch_hashes = hashes[i:i+BATCH_SIZE]
            batch_sources = sources[i:i+BATCH_SIZE]
            batch_bank_ids = bank_ids[i:i+BATCH_SIZE]

            preds = sentiment_model.predict_batch(batch_texts)

            bulk_rows = []

            for j, p in enumerate(preds):

                score = p["score"]

                if p["label"] == "NEGATIVE":
                    score = -score

                final_score, contradiction = fuse_sentiment(
                    score,
                    batch_ratings[j]
                )

                label = sentiment_label(final_score)

                bulk_rows.append(
                    (
                        batch_bank_ids[j],
                        bank,
                        year,
                        batch_texts[j],
                        batch_hashes[j],
                        batch_ratings[j],
                        final_score,
                        label,
                        batch_sources[j]
                    )
                )

                fused_scores.append(final_score)

                if contradiction:
                    contradiction_count += 1

            bulk_insert_reviews(cursor, bulk_rows)
            conn.commit()

        if not fused_scores:
            continue

        yearly_score = sum(fused_scores) / len(fused_scores)

        contradiction_ratio = contradiction_count / len(items)

        year_sentiments[year] = yearly_score
        yearly_contradictions[year] = contradiction_ratio

        cursor.execute(
            """
            INSERT OR REPLACE INTO sentiment_scores
            (bank_id, bank_name, year, sentiment, contradiction_ratio)
            VALUES (?, ?, ?, ?, ?)
            """,
            (bank_id, bank, year, yearly_score, contradiction_ratio)
        )

        conn.commit()

        print(f"{bank} {year} → {yearly_score:.3f}")

    conn.close()

    trend = detect_trend(year_sentiments)

    return bank, {
        "yearly_sentiment": year_sentiments,
        "trend_direction": trend,
        "yearly_contradiction_ratio": yearly_contradictions
    }


# --------------------------------------------------
# Main engine
# --------------------------------------------------

def main():

    banks = discover_review_folders(BASE_CORP_PATH)

    trend_results = {}

    print("\n🚀 Running Ultra Optimized Sentiment Engine\n")

    workers = max(1, multiprocessing.cpu_count() - 1)

    with ProcessPoolExecutor(max_workers=workers) as executor:

        results = executor.map(process_bank, banks.items())

        for bank, result in results:

            if result:
                trend_results[bank] = result

    with open(JSON_OUTPUT_PATH, "w") as f:
        json.dump(trend_results, f, indent=4)

    print("\n📄 JSON trend data saved:", JSON_OUTPUT_PATH)
    print("\n✅ Sentiment trend completed")


if __name__ == "__main__":
    main()