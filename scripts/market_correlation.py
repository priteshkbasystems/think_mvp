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
# DISCOVER ALL BANKS
# ==========================================

def discover_all_banks(base_path):

    banks = {}

    for bank_folder in os.listdir(base_path):
        bank_path = os.path.join(base_path, bank_folder)

        if not os.path.isdir(bank_path):
            continue

        display_name = bank_folder.replace("_", " ")
        stock_path = os.path.join(bank_path, "stock_price")

        stock_file = None

        if os.path.exists(stock_path):
            for file in os.listdir(stock_path):
                if file.endswith(".csv"):
                    stock_file = os.path.join(stock_path, file)

        banks[display_name] = stock_file

    return banks


# ==========================================
# CALCULATE YEARLY RETURNS
# ==========================================

def compute_yearly_returns(csv_path):

    df = pd.read_csv(csv_path)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

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
# CORRELATION + YEAR ALIGNMENT
# ==========================================

def compute_correlation(sentiment_dict, return_dict, lag=0):

    aligned_data = []

    for year in sentiment_dict:
        target_year = year + lag

        if target_year in return_dict:
            aligned_data.append(
                (year, sentiment_dict[year], return_dict[target_year])
            )

    if len(aligned_data) < 2:
        return None, []

    x = [item[1] for item in aligned_data]
    y = [item[2] for item in aligned_data]

    corr, _ = pearsonr(x, y)

    return corr, aligned_data


# ==========================================
# INTERPRETATION
# ==========================================

def interpret_correlation(value, timing):

    abs_val = abs(value)

    if abs_val >= 0.7:
        strength = "strong"
    elif abs_val >= 0.4:
        strength = "moderate"
    elif abs_val >= 0.2:
        strength = "weak"
    else:
        strength = "very weak"

    direction = "positive" if value > 0 else "inverse"

    if timing == "same":
        period = "within the same year"
    else:
        period = "in the following year"

    explanation = (
        f"There is a {strength} {direction} relationship between customer sentiment "
        f"and stock returns {period}. "
    )

    if value > 0 and timing == "next":
        explanation += (
            "This suggests that improvements in customer perception may precede "
            "stronger market performance."
        )
    elif value < 0:
        explanation += (
            "This indicates that market performance may be influenced more by "
            "external macroeconomic or structural factors."
        )

    return explanation


# ==========================================
# MAIN
# ==========================================

def main():

    print("\n📈 Running Market Correlation Engine...\n")

    sentiment_data = load_sentiment_data()
    banks = discover_all_banks(BASE_CORP_PATH)

    report_lines = []
    report_lines.append("SENTIMENT → STOCK MARKET IMPACT REPORT")
    report_lines.append("======================================\n")

    for bank, stock_path in banks.items():

        report_lines.append(f"\n🏦 {bank}")

        sentiment_dict = sentiment_data.get(bank)

        if sentiment_dict is None:
            report_lines.append("Sentiment data not available.")
            continue

        if not stock_path:
            report_lines.append("Stock price data not available.")
            continue

        yearly_returns = compute_yearly_returns(stock_path)

        # SAME YEAR
        same_corr, same_data = compute_correlation(
            sentiment_dict,
            yearly_returns,
            lag=0
        )

        if same_corr is not None:
            report_lines.append(
                f"\nSame Year Correlation: {same_corr:.3f}"
            )

            report_lines.append("Years Analyzed:")

            for year, sent, ret in same_data:
                report_lines.append(
                    f"  {year} → Sentiment: {sent:.3f} | Return: {ret:.3f}"
                )

            report_lines.append(
                interpret_correlation(same_corr, "same")
            )
        else:
            report_lines.append("\nSame Year: Insufficient overlapping data.")

        # NEXT YEAR
        next_corr, next_data = compute_correlation(
            sentiment_dict,
            yearly_returns,
            lag=1
        )

        if next_corr is not None:
            report_lines.append(
                f"\nNext Year Correlation: {next_corr:.3f}"
            )

            report_lines.append("Years Analyzed (Sentiment → Following Year Return):")

            for year, sent, ret in next_data:
                report_lines.append(
                    f"  {year} → Sentiment: {sent:.3f} | {year+1} Return: {ret:.3f}"
                )

            report_lines.append(
                interpret_correlation(next_corr, "next")
            )
        else:
            report_lines.append("\nNext Year: Insufficient overlapping data.")

        report_lines.append("")

    final_report = "\n".join(report_lines)

    with open(OUTPUT_PATH, "w") as f:
        f.write(final_report)

    print("\n📄 Report saved to:", OUTPUT_PATH)
    print("\n" + final_report)


if __name__ == "__main__":
    main()