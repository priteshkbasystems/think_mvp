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
# AUTO DISCOVER ALL BANKS + STOCK FILES
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
# CLEAN STOCK DATA & CALCULATE YEARLY RETURN
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
# CORRELATION
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
# INTERPRETATION FUNCTION
# ==========================================

def interpret_correlation(value, timing):

    if value is None:
        return "Insufficient overlapping historical data to determine statistical relationship."

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
            "This suggests that improved customer perception may contribute "
            "to stronger future market performance."
        )
    elif value < 0 and timing == "same":
        explanation += (
            "This indicates that short-term market performance may be driven "
            "by macroeconomic or structural factors rather than customer sentiment."
        )
    elif value < 0:
        explanation += (
            "This suggests that positive sentiment does not necessarily translate "
            "into improved stock returns."
        )
    else:
        explanation += (
            "This indicates alignment between customer perception and investor response."
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

        print(f"Analyzing {bank}...")

        sentiment_dict = sentiment_data.get(bank)

        report_lines.append(f"\n🏦 {bank}")

        if sentiment_dict is None:
            report_lines.append("Sentiment data not available.")
            continue

        if not stock_path:
            report_lines.append("Stock price data not found.")
            continue

        yearly_returns = compute_yearly_returns(stock_path)

        same_year_corr = compute_correlation(sentiment_dict, yearly_returns, lag=0)
        next_year_corr = compute_correlation(sentiment_dict, yearly_returns, lag=1)

        # SAME YEAR
        if same_year_corr is not None:
            report_lines.append(
                f"Sentiment → Same Year Stock Return Correlation: {same_year_corr:.3f}"
            )
            report_lines.append(interpret_correlation(same_year_corr, "same"))
        else:
            report_lines.append("Sentiment → Same Year: Insufficient Data")

        # NEXT YEAR
        if next_year_corr is not None:
            report_lines.append(
                f"Sentiment → Next Year Stock Return Correlation: {next_year_corr:.3f}"
            )
            report_lines.append(interpret_correlation(next_year_corr, "next"))
        else:
            report_lines.append("Sentiment → Next Year: Insufficient Data")

        report_lines.append("")

    final_report = "\n".join(report_lines)

    with open(OUTPUT_PATH, "w") as f:
        f.write(final_report)

    print("\n📄 Report saved to:", OUTPUT_PATH)
    print("\n" + final_report)


if __name__ == "__main__":
    main()