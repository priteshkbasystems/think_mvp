import os
import json
import pandas as pd
from collections import defaultdict

from models.sentiment_model import SentimentModel
from scripts.utils.sentiment_utils import sentiment_label
from scripts.db_cache import save_sentiment_score
from scripts.db_cache import save_review_sentiment
from scripts.progress_tracker import ProgressTracker


BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_report.txt"
JSON_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"

TEXT_WEIGHT = 0.7
RATING_WEIGHT = 0.3
CONTRADICTION_THRESHOLD = 0.8

BATCH_SIZE = 128
STEP_NAME = "STEP 2 — SENTIMENT TREND"


# -----------------------------------------
# Normalize rating
# -----------------------------------------

def normalize_rating(star_rating):
    return (star_rating - 3) / 2


# -----------------------------------------
# Fuse sentiment
# -----------------------------------------

def fuse_sentiment(text_score, rating):

    if rating is None or pd.isna(rating):
        return text_score, False

    normalized_rating = normalize_rating(rating)

    final = TEXT_WEIGHT * text_score + RATING_WEIGHT * normalized_rating

    contradiction = abs(text_score - normalized_rating) > CONTRADICTION_THRESHOLD

    return final, contradiction


# -----------------------------------------
# Discover review folders
# -----------------------------------------

def discover_review_folders(base_path):

    banks = {}

    if not os.path.exists(base_path):
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


# -----------------------------------------
# Load reviews (ALL Excel Sheets)
# -----------------------------------------

def load_reviews(folder):

    data = []

    for file in os.listdir(folder):

        if not file.endswith(".xlsx"):
            continue

        path = os.path.join(folder, file)

        try:
            xls = pd.ExcelFile(path)
        except Exception as e:
            print("⚠ Cannot open file:", file, e)
            continue

        print(f"\n📄 Loading file: {file}")

        for sheet in xls.sheet_names:

            try:
                df = pd.read_excel(xls, sheet_name=sheet)
            except:
                continue

            print(f"   → Sheet: {sheet}")

            if "Date" not in df.columns or "review" not in df.columns:
                print("   ⚠ Required columns missing. Skipping.")
                continue

            df["Date"] = pd.to_datetime(
                df["Date"],
                errors="coerce",
                dayfirst=True
            )

            df = df.dropna(subset=["Date"])

            df["review"] = df["review"].astype(str)
            df = df[df["review"].str.strip() != ""]

            if "Rating" in df.columns:
                df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
            else:
                df["Rating"] = None

            for _, row in df.iterrows():

                year = int(row["Date"].year)

                data.append({
                    "year": year,
                    "text": row["review"],
                    "rating": row["Rating"]
                })

    return data


# -----------------------------------------
# Detect trend
# -----------------------------------------

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


# -----------------------------------------
# Main Engine
# -----------------------------------------

def main():

    sentiment_model = SentimentModel()
    tracker = ProgressTracker()

    banks = discover_review_folders(BASE_CORP_PATH)

    trend_results = {}
    report_lines = []

    print("\n🚀 Running Optimized Sentiment Engine\n")

    for bank, path in banks.items():

        data = load_reviews(path)

        if len(data) == 0:
            print("⚠ No reviews:", bank)
            continue

        year_groups = defaultdict(list)

        for d in data:
            year_groups[d["year"]].append(d)

        year_sentiments = {}
        yearly_contradictions = {}

        print("\n🏦", bank)

        report_lines.append("\n" + bank)
        report_lines.append("------------------------")

        for year in sorted(year_groups.keys()):

            items = year_groups[year]

            start = tracker.get_progress(STEP_NAME, bank, year)

            items = items[start:]

            fused_scores = []
            contradiction_count = 0

            texts = [i["text"] for i in items]
            ratings = [i["rating"] for i in items]

            total_processed = start

            for i in range(0, len(texts), BATCH_SIZE):

                batch_texts = texts[i:i+BATCH_SIZE]
                batch_ratings = ratings[i:i+BATCH_SIZE]

                preds = sentiment_model.predict_batch(batch_texts)

                for j, p in enumerate(preds):

                    score = p["score"]

                    if p["label"] == "NEGATIVE":
                        score = -score

                    final_score, contradiction = fuse_sentiment(
                        score,
                        batch_ratings[j]
                    )

                    label = sentiment_label(final_score)

                    save_review_sentiment(
                        bank,
                        year,
                        batch_texts[j],
                        batch_ratings[j],
                        final_score,
                        label
                    )

                    fused_scores.append(final_score)

                    if contradiction:
                        contradiction_count += 1

                total_processed += len(batch_texts)

                tracker.save_progress(
                    STEP_NAME,
                    bank,
                    year,
                    total_processed
                )

            if not fused_scores:
                continue

            yearly_score = sum(fused_scores) / len(fused_scores)

            contradiction_ratio = contradiction_count / len(items)

            year_sentiments[year] = yearly_score
            yearly_contradictions[year] = contradiction_ratio

            save_sentiment_score(bank, year, yearly_score, contradiction_ratio)

            label = sentiment_label(yearly_score)

            print(f"{year} → {yearly_score:.3f} ({label})")

            report_lines.append(
                f"{year} → {yearly_score:.3f} ({label}) "
                f"(Contradiction: {contradiction_ratio:.2%})"
            )

        trend = detect_trend(year_sentiments)

        report_lines.append(f"Trend: {trend}")

        trend_results[bank] = {
            "yearly_sentiment": year_sentiments,
            "trend_direction": trend,
            "yearly_contradiction_ratio": yearly_contradictions
        }

    with open(OUTPUT_PATH, "w") as f:
        f.write("\n".join(report_lines))

    with open(JSON_OUTPUT_PATH, "w") as f:
        json.dump(trend_results, f, indent=4)

    print("\n📄 Trend report saved:", OUTPUT_PATH)
    print("📄 JSON data saved:", JSON_OUTPUT_PATH)

    print("\n✅ Sentiment trend completed")


if __name__ == "__main__":
    main()