import os
import json
import pandas as pd
import numpy as np
from scipy.stats import pearsonr


# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
TREND_JSON_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"
OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/market_correlation_report.txt"


# ==========================================
# LOAD SENTIMENT DATA
# ==========================================

def load_sentiment_data():
    if not os.path.exists(TREND_JSON_PATH):
        print("⚠ Sentiment JSON not found.")
        return {}

    with open(TREND_JSON_PATH, "r") as f:
        raw = json.load(f)

    sentiment_data = {}

    for bank, data in raw.items():
        sentiment_data[bank] = {
            int(year): score
            for year, score in data["yearly_sentiment"].items()
        }

    return sentiment_data


# ==========================================
# AUTO DISCOVER STOCK FILE
# ==========================================

def discover_stock_files(base_path):
    stock_files = {}

    for bank_folder in os.listdir(base_path):
        bank_path = os.path.join(base_path, bank_folder)
        stock_path = os.path.join(bank_path, "stock_price")

        if not os.path.exists(stock_path):
            continue

        for file in os.listdir(stock_path):
            if file.endswith(".csv"):
                display_name = bank_folder.replace("_", " ")
                stock_files[display_name] = os.path.join(stock_path, file)

    return stock_files


# ==========================================
# CLEAN STOCK DATA & CALCULATE YEARLY RETURN
# ==========================================

def compute_yearly_returns(csv_path):

    df = pd.read_csv(csv_path)

    # Clean columns
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    # Clean price (remove commas)
    df["Price"] = (
        df["Price"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    df["Year"] = df["Date"].dt.year

    yearly_returns = {}

    for year in df["Year"].unique():
        year_df = df[df["Year"] == year].sort_values("Date")

        if len(year_df) < 2:
            continue

        first_price = year_df.iloc[0]["Price"]
        last_price = year_df.iloc[-1]["Price"]

        yearly_return = (last_price - first_price) / first_price
        yearly_returns[int(year)] = yearly_return

    return yearly_returns


# ==========================================
# CORRELATION LOGIC
# ==========================================

def compute_correlation(sentiment_dict, return_dict, lag=0):

    common_years = sorted(
        set(sentiment_dict.keys()) &
        set(y - lag for y in return_dict.keys())
    )

    if len(common_years) < 2:
        return None

    x = []
    y = []

    for year in common_years:
        if (year + lag) in return_dict:
            x.append(sentiment_dict[year])
            y.append(return_dict[year + lag])

    if len(x) < 2:
        return None

    corr, _ = pearsonr(x, y)
    return corr


# ==========================================
# MAIN
# ==========================================

def main():

    print("\n📈 Running Market Correlation Engine...\n")

    sentiment_data = load_sentiment_data()
    stock_files = discover_stock_files(BASE_CORP_PATH)

    report_lines = []
    report_lines.append("SENTIMENT → STOCK MARKET CORRELATION REPORT")
    report_lines.append("===========================================\n")

    for bank, sentiment_dict in sentiment_data.items():

        print(f"Analyzing {bank}...")

        stock_path = stock_files.get(bank)

        if not stock_path:
            report_lines.append(f"\n🏦 {bank}")
            report_lines.append("Stock data not found.\n")
            continue

        yearly_returns = compute_yearly_returns(stock_path)

        same_year_corr = compute_correlation(
            sentiment_dict,
            yearly_returns,
            lag=0
        )

        next_year_corr = compute_correlation(
            sentiment_dict,
            yearly_returns,
            lag=1
        )

        report_lines.append(f"\n🏦 {bank}")

        if same_year_corr is not None:
            report_lines.append(
                f"Sentiment → Same Year Stock Return: {same_year_corr:.3f}"
            )
        else:
            report_lines.append("Sentiment → Same Year: Insufficient Data")

        if next_year_corr is not None:
            report_lines.append(
                f"Sentiment → Next Year Stock Return: {next_year_corr:.3f}"
            )
        else:
            report_lines.append("Sentiment → Next Year: Insufficient Data")

    final_report = "\n".join(report_lines)

    with open(OUTPUT_PATH, "w") as f:
        f.write(final_report)

    print("\n📄 Report saved to:", OUTPUT_PATH)
    print("\n" + final_report)


if __name__ == "__main__":
    main()