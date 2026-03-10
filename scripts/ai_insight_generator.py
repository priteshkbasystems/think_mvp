import os
import json
import sqlite3

from scripts.utils.sentiment_utils import sentiment_label


# ==========================================
# CONFIG
# ==========================================

BASE_OUTPUT = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output"

DB_PATH = f"{BASE_OUTPUT}/transformation_cache.db"
TREND_JSON = f"{BASE_OUTPUT}/bank_trend_data.json"
OUTPUT_PATH = f"{BASE_OUTPUT}/executive_ai_insights.txt"


# ==========================================
# LOAD SENTIMENT DATA
# ==========================================

def load_sentiment():

    if not os.path.exists(TREND_JSON):
        print("⚠ Sentiment JSON not found")
        return {}

    with open(TREND_JSON, "r") as f:
        data = json.load(f)

    sentiment = {}

    for bank, details in data.items():

        sentiment[bank] = {
            int(year): score
            for year, score in details["yearly_sentiment"].items()
        }

    return sentiment


# ==========================================
# LOAD STOCK RETURNS
# ==========================================

def load_stock_returns():

    if not os.path.exists(DB_PATH):
        print("⚠ Database not found:", DB_PATH)
        return {}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT bank_name, year, return FROM stock_returns"
        )

        rows = cursor.fetchall()

    except Exception:
        print("⚠ stock_returns table missing in database")
        conn.close()
        return {}

    conn.close()

    stock = {}

    for bank, year, value in rows:

        if bank not in stock:
            stock[bank] = {}

        stock[bank][year] = value

    return stock


# ==========================================
# MARKET LABEL
# ==========================================

def market_label(value):

    if value >= 0.05:
        return "positive"

    elif value <= -0.05:
        return "negative"

    else:
        return "neutral"


# ==========================================
# GENERATE STRATEGIC INSIGHTS
# ==========================================

def generate_insights(sentiment_data, stock_data):

    report = []

    report.append("AI EXECUTIVE STRATEGIC INSIGHT REPORT")
    report.append("=====================================\n")

    for bank in sorted(sentiment_data.keys()):

        report.append(f"\n🏦 {bank}")
        report.append("-" * (len(bank) + 3))

        sentiment_years = sentiment_data.get(bank, {})
        stock_years = stock_data.get(bank, {})

        all_years = sorted(
            set(sentiment_years.keys()) |
            set(stock_years.keys())
        )

        for year in all_years:

            report.append(f"\n📅 {year}")

            s = sentiment_years.get(year)
            r = stock_years.get(year)

            # ==========================
            # SENTIMENT
            # ==========================

            if s is not None:

                s_label = sentiment_label(s)

                report.append(
                    f"Customer sentiment was {s_label} ({s:.3f})."
                )

            else:

                report.append(
                    "Customer sentiment data not available."
                )

            # ==========================
            # MARKET PERFORMANCE
            # ==========================

            if r is not None:

                r_label = market_label(r)

                report.append(
                    f"Stock market performance was {r_label} ({r:.3f})."
                )

            else:

                report.append(
                    "Market return data not available."
                )

            # ==========================
            # STRATEGIC INTERPRETATION
            # ==========================

            if s is not None and r is not None:

                sentiment_state = sentiment_label(s)
                market_state = market_label(r)

                if sentiment_state == "Positive" and market_state == "positive":

                    report.append(
                        "Insight: Positive customer perception aligned with strong investor confidence."
                    )

                elif sentiment_state == "Negative" and market_state == "positive":

                    report.append(
                        "Insight: Despite negative customer sentiment, investors maintained confidence in the bank's long-term strategy."
                    )

                elif sentiment_state == "Positive" and market_state == "negative":

                    report.append(
                        "Insight: Customer satisfaction improved but the market did not reflect this improvement."
                    )

                elif sentiment_state == "Neutral" and market_state == "positive":

                    report.append(
                        "Insight: Market performance improved while customer sentiment remained neutral."
                    )

                elif sentiment_state == "Neutral" and market_state == "negative":

                    report.append(
                        "Insight: Market declined despite neutral customer sentiment."
                    )

                else:

                    report.append(
                        "Insight: Negative sentiment and declining stock performance suggest operational or strategic challenges."
                    )

        report.append("\n" + "=" * 60)

    return "\n".join(report)


# ==========================================
# MAIN
# ==========================================

def main():

    print("\n🤖 Generating AI Executive Insights...\n")

    sentiment = load_sentiment()
    stock = load_stock_returns()

    report = generate_insights(sentiment, stock)

    with open(OUTPUT_PATH, "w") as f:
        f.write(report)

    print("✅ Executive insight report generated.")
    print("Saved to:", OUTPUT_PATH)


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":
    main()