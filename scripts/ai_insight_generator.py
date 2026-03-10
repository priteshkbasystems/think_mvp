import os
import json
import sqlite3

BASE_OUTPUT = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output"
DB_PATH = f"{BASE_OUTPUT}/transformation_cache.db"
TREND_JSON = f"{BASE_OUTPUT}/bank_trend_data.json"
OUTPUT_PATH = f"{BASE_OUTPUT}/executive_ai_insights.txt"


# ==========================================
# LOAD SENTIMENT DATA
# ==========================================

def load_sentiment():

    if not os.path.exists(TREND_JSON):
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

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT bank_name, year, return FROM stock_returns")

    rows = cursor.fetchall()
    conn.close()

    stock = {}

    for bank, year, value in rows:

        if bank not in stock:
            stock[bank] = {}

        stock[bank][year] = value

    return stock


# ==========================================
# SENTIMENT LABEL
# ==========================================

def sentiment_label(value):

    if value > 0.2:
        return "positive"
    elif value < -0.2:
        return "negative"
    else:
        return "neutral"


# ==========================================
# MARKET LABEL
# ==========================================

def market_label(value):

    if value > 0.05:
        return "positive"
    elif value < -0.05:
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

    for bank in sentiment_data:

        report.append(f"\n🏦 {bank}")
        report.append("-" * (len(bank) + 3))

        sentiment_years = sentiment_data.get(bank, {})
        stock_years = stock_data.get(bank, {})

        all_years = sorted(set(list(sentiment_years.keys()) + list(stock_years.keys())))

        for year in all_years:

            report.append(f"\n📅 {year}")

            s = sentiment_years.get(year)
            r = stock_years.get(year)

            if s is not None:
                s_label = sentiment_label(s)
                report.append(f"Customer sentiment was {s_label} ({s:.3f}).")
            else:
                report.append("Customer sentiment data not available.")

            if r is not None:
                r_label = market_label(r)
                report.append(f"Stock market performance was {r_label} ({r:.3f}).")
            else:
                report.append("Market return data not available.")

            # Strategic interpretation
            if s is not None and r is not None:

                if s > 0 and r > 0:
                    report.append(
                        "Insight: Positive customer perception aligned with strong investor confidence."
                    )

                elif s < 0 and r > 0:
                    report.append(
                        "Insight: Despite negative customer sentiment, investors maintained confidence in the bank's long-term strategy."
                    )

                elif s > 0 and r < 0:
                    report.append(
                        "Insight: Customer satisfaction improved but the market did not reflect this improvement."
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


if __name__ == "__main__":
    main()