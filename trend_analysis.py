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
# LOAD REVIEWS
# ==========================================

def load_reviews_with_dates(folder_path):
    data = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            full_path = os.path.join(folder_path, file)
            df = pd.read_excel(full_path)

            print(f"\n📄 Loading: {file}")
            print("Columns:", list(df.columns))

            if "Date" not in df.columns or "review" not in df.columns:
                print("⚠ Required columns not found. Skipping file.")
                continue

            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            df["review"] = df["review"].astype(str)
            df = df[df["review"].str.strip() != ""]

            for _, row in df.iterrows():
                data.append({
                    "year": int(row["Date"].year),
                    "text": row["review"]
                })

    return data


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
    report_lines.append("THAI BANK SENTIMENT TREND REPORT")
    report_lines.append("=================================\n")

    trend_results = {}

    print("\n📈 Running Yearly Sentiment Trend Engine...\n")

    for bank, path in banks.items():

        if not os.path.exists(path):
            continue

        data = load_reviews_with_dates(path)

        if len(data) == 0:
            print(f"⚠ No valid reviews found for {bank}")
            continue

        year_groups = defaultdict(list)
        for item in data:
            year_groups[item["year"]].append(item["text"])

        year_sentiments = {}

        print(f"\n🏦 {bank}")
        print("----------------------------")

        for year in sorted(year_groups.keys()):
            texts = year_groups[year]

            try:
                _, _, metrics = processor.process(texts)
                sentiment_score = float(metrics["overall_sentiment"])
            except Exception:
                sentiment_score = 0.0

            year_sentiments[year] = sentiment_score

            print(f"{year} → {sentiment_score:.3f}")

        trend_direction = detect_trend(year_sentiments)

        print(f"Trend: {trend_direction}")

        trend_results[bank] = {
            "yearly_sentiment": year_sentiments,
            "trend_direction": trend_direction
        }

        report_lines.append(f"\n{bank}")
        report_lines.append("----------------------------")

        for year in sorted(year_sentiments.keys()):
            report_lines.append(f"{year} → {year_sentiments[year]:.3f}")

        report_lines.append(f"Trend: {trend_direction}\n")

    # Save Text Report
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    # Save JSON for Correlation Engine
    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(trend_results, f, indent=4)

    print("\n📄 Trend report saved to:", OUTPUT_PATH)
    print("📄 JSON trend data saved to:", JSON_OUTPUT_PATH)


if __name__ == "__main__":
    main()