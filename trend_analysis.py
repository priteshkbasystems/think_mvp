import os
import pandas as pd
from collections import defaultdict
from scripts.processor import TextProcessor


BANK_PATHS = {
    "Krungthai Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/Krungthai_Bank/Reviews",
    "Kasikornbank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/KBank/Reviews",
    "SCB_Pre2022 Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/SCB_Pre2022/Reviews",
}

OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_report.txt"


def load_reviews_with_dates(folder_path):
    data = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            df = pd.read_excel(os.path.join(folder_path, file))

            if "Date" not in df.columns:
                print("⚠ Date column not found in", file)
                continue

            review_col = None
            for col in df.columns:
                if "review" in col.lower():
                    review_col = col
                    break

            if review_col is None:
                print("⚠ Review column not found in", file)
                continue

            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            for _, row in df.iterrows():
                data.append({
                    "year": row["Date"].year,
                    "text": str(row[review_col])
                })

    return data


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


def main():
    processor = TextProcessor()
    report_lines = []
    report_lines.append("THAI BANK SENTIMENT TREND REPORT")
    report_lines.append("=================================\n")

    print("\n📈 Running Yearly Trend Analysis...\n")

    for bank, path in BANK_PATHS.items():

        if not os.path.exists(path):
            print(f"⚠ Folder not found for {bank}")
            continue

        data = load_reviews_with_dates(path)

        if len(data) == 0:
            print(f"⚠ No valid dated reviews for {bank}")
            continue

        year_groups = defaultdict(list)

        for item in data:
            year_groups[item["year"]].append(item["text"])

        year_sentiments = {}

        for year, texts in year_groups.items():
            _, _, metrics = processor.process(texts)
            year_sentiments[year] = metrics["overall_sentiment"]

        trend_direction = detect_trend(year_sentiments)

        report_lines.append(f"\n{bank}")
        report_lines.append("----------------------------")

        print(f"\n{bank}")
        print("----------------------------")

        for year in sorted(year_sentiments.keys()):
            sentiment_score = year_sentiments[year]
            report_lines.append(f"{year} → {sentiment_score:.3f}")
            print(f"{year} → {sentiment_score:.3f}")

        report_lines.append(f"Trend: {trend_direction}\n")
        print(f"Trend: {trend_direction}")

    report_text = "\n".join(report_lines)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)

    print("\n📄 Trend report saved to:", OUTPUT_PATH)


if __name__ == "__main__":
    main()