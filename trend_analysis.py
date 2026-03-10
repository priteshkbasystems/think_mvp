import os
import json
import pandas as pd
from collections import defaultdict
from scripts.processor import TextProcessor


# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_report.txt"
JSON_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"

TEXT_WEIGHT = 0.7
RATING_WEIGHT = 0.3
CONTRADICTION_THRESHOLD = 0.8


# ==========================================
# AUTO DISCOVER BANKS
# ==========================================

def discover_review_folders(base_path):
    banks = {}

    for bank_folder in os.listdir(base_path):
        bank_path = os.path.join(base_path, bank_folder)

        if not os.path.isdir(bank_path):
            continue

        reviews_path = os.path.join(bank_path, "Reviews")

        if os.path.exists(reviews_path):
            banks[bank_folder.replace("_", " ")] = reviews_path

    return banks


# ==========================================
# NORMALIZE STAR RATING (1–5 → -1 to +1)
# ==========================================

def normalize_rating(star_rating):
    return (star_rating - 3) / 2


# ==========================================
# LOAD REVIEWS
# ==========================================

def load_reviews_with_dates(folder_path):

    data = []

    for file in os.listdir(folder_path):

        if file.endswith(".xlsx"):

            full_path = os.path.join(folder_path, file)

            print(f"\n📄 Loading file: {file}")

            try:
                xls = pd.ExcelFile(full_path)
            except Exception as e:
                print("⚠ Unable to open file:", e)
                continue

            # Loop through ALL sheets
            for sheet in xls.sheet_names:

                try:
                    df = pd.read_excel(xls, sheet_name=sheet)
                except Exception:
                    continue

                print(f"   → Sheet: {sheet}")
                print("   Columns:", list(df.columns))

                if "Date" not in df.columns or "review" not in df.columns:
                    print("   ⚠ Required columns not found. Skipping sheet.")
                    continue

                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"])

                df["review"] = df["review"].astype(str)
                df = df[df["review"].str.strip() != ""]

                # Rating optional
                if "Rating" in df.columns:
                    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
                else:
                    df["Rating"] = None

                for _, row in df.iterrows():

                    data.append({
                        "year": int(row["Date"].year),
                        "text": row["review"],
                        "rating": row["Rating"]
                    })

    return data


# ==========================================
# FUSION LOGIC
# ==========================================

def fuse_sentiment(text_score, rating):

    if rating is None or pd.isna(rating):
        return text_score, False  # no contradiction

    normalized_rating = normalize_rating(rating)

    final_sentiment = (
        TEXT_WEIGHT * text_score +
        RATING_WEIGHT * normalized_rating
    )

    difference = abs(text_score - normalized_rating)
    contradiction = difference > CONTRADICTION_THRESHOLD

    return final_sentiment, contradiction


# ==========================================
# TREND DETECTION
# ==========================================

def detect_trend(year_sentiments):
    years = sorted(year_sentiments.keys())
    values = [year_sentiments[y] for y in years]

    if len(values) < 2:
        return "Insufficient Data"

    total_change = sum(
        values[i] - values[i - 1] for i in range(1, len(values))
    )

    avg_change = total_change / (len(values) - 1)
    threshold = 0.01

    if avg_change > threshold:
        return "Improving"
    elif avg_change < -threshold:
        return "Declining"
    else:
        return "Stable"


# ==========================================
# MAIN ENGINE
# ==========================================

def main():

    processor = TextProcessor()
    banks = discover_review_folders(BASE_CORP_PATH)

    report_lines = []
    report_lines.append("THAI BANK SENTIMENT TREND REPORT (TEXT + STAR FUSION)")
    report_lines.append("=====================================================\n")

    trend_results = {}

    print("\n📈 Running Yearly Sentiment Trend Engine...\n")

    for bank, path in banks.items():

        data = load_reviews_with_dates(path)

        if len(data) == 0:
            continue

        year_groups = defaultdict(list)
        for item in data:
            year_groups[item["year"]].append(item)

        year_sentiments = {}
        yearly_contradictions = {}

        print(f"\n🏦 {bank}")
        print("----------------------------")

        for year in sorted(year_groups.keys()):

            items = year_groups[year]
            texts = [x["text"] for x in items]

            try:
                _, _, metrics = processor.process(texts)
                text_score = float(metrics["overall_sentiment"])
            except Exception:
                text_score = 0.0

            # Apply fusion per review
            fused_scores = []
            contradiction_count = 0

            for item in items:
                final_score, contradiction = fuse_sentiment(
                    text_score,
                    item["rating"]
                )
                fused_scores.append(final_score)

                if contradiction:
                    contradiction_count += 1

            yearly_score = sum(fused_scores) / len(fused_scores)
            contradiction_ratio = contradiction_count / len(items)

            year_sentiments[year] = yearly_score
            yearly_contradictions[year] = contradiction_ratio

            print(f"{year} → {yearly_score:.3f} | Contradiction Rate: {contradiction_ratio:.2%}")

        trend_direction = detect_trend(year_sentiments)

        trend_results[bank] = {
            "yearly_sentiment": year_sentiments,
            "trend_direction": trend_direction,
            "yearly_contradiction_ratio": yearly_contradictions
        }

        report_lines.append(f"\n{bank}")
        report_lines.append("----------------------------")

        for year in sorted(year_sentiments.keys()):
            report_lines.append(
                f"{year} → {year_sentiments[year]:.3f} "
                f"(Contradiction: {yearly_contradictions[year]:.2%})"
            )

        report_lines.append(f"Trend: {trend_direction}\n")

    # Save reports
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(trend_results, f, indent=4)

    print("\n📄 Trend report saved to:", OUTPUT_PATH)
    print("📄 JSON trend data saved to:", JSON_OUTPUT_PATH)


if __name__ == "__main__":
    main()