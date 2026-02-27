import os
import pandas as pd
from collections import defaultdict
from scripts.processor import TextProcessor


# ==============================
# CONFIG
# ==============================

BANK_PATHS = {
    "Krungthai Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/Krungthai_Bank/Reviews",
    "Kasikornbank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/KBank/Reviews",
    "SCB_Pre2022 Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/SCB_Pre2022/Reviews",
}

OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_report.txt"


# ==============================
# LOAD REVIEWS (Based on Your File Structure)
# ==============================

def load_reviews_with_dates(folder_path):
    data = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            full_path = os.path.join(folder_path, file)
            df = pd.read_excel(full_path)

            print(f"\n📄 Loading: {file}")
            print("Columns:", list(df.columns))

            # Ensure required columns exist
            if "Date" not in df.columns or "review" not in df.columns:
                print("⚠ Required columns not found. Skipping file.")
                continue

            # Convert Date column safely
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            # Clean review text
            df["review"] = df["review"].astype(str)
            df = df[df["review"].str.strip() != ""]

            for _, row in df.iterrows():
                data.append({
                    "year": row["Date"].year,
                    "text": row["review"]
                })

    return data


# ==============================
# TREND DETECTION LOGIC
# ==============================

def detect_trend(year_sentiments):
    years = sorted(year_sentiments.keys())
    values = [year_sentiments[y] for y in years]

    if len(values) < 2:
        return "Insufficient Data"

    if values[-1] > values[0]:
        return "Improving"
    elif values[-1] < values[0]:
        return "Declining"
    else:
        return "Stable"


# ==============================
# MAIN ENGINE
# ==============================

def main():

    processor = TextProcessor()

    report_lines = []
    report_lines.append("THAI BANK SENTIMENT TREND REPORT")
    report_lines.append("=================================\n")

    print("\n📈 Running Yearly Sentiment Trend Engine...\n")

    for bank, path in BANK_PATHS.items():

        if not os.path.exists(path):
            print(f"⚠ Folder not found for {bank}")
            continue

        data = load_reviews_with_dates(path)

        if len(data) == 0:
            print(f"⚠ No valid reviews found for {bank}")
            continue

        # Group by year
        year_groups = defaultdict(list)
        for item in data:
            year_groups[item["year"]].append(item["text"])

        year_sentiments = {}

        print(f"\n🏦 {bank}")
        print("----------------------------")

        for year in sorted(year_groups.keys()):
            texts = year_groups[year]

            _, _, metrics = processor.process(texts)

            sentiment_score = metrics["overall_sentiment"]
            year_sentiments[year] = sentiment_score

            print(f"{year} → {sentiment_score:.3f}")

        trend_direction = detect_trend(year_sentiments)

        print(f"Trend: {trend_direction}")

        # Add to report
        report_lines.append(f"\n{bank}")
        report_lines.append("----------------------------")

        for year in sorted(year_sentiments.keys()):
            report_lines.append(f"{year} → {year_sentiments[year]:.3f}")

        report_lines.append(f"Trend: {trend_direction}\n")

    # Save Report
    report_text = "\n".join(report_lines)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)

    print("\n📄 Trend report saved to:", OUTPUT_PATH)


if __name__ == "__main__":
    main()