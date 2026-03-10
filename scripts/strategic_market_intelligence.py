import os
import json
import sqlite3

from scripts.utils.sentiment_utils import sentiment_label


# ==========================================
# CONFIG
# ==========================================

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"

TREND_JSON_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"

OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/strategic_market_intelligence_report.txt"


# ==========================================
# LOAD SENTIMENT DATA
# ==========================================

def load_sentiment_data():

    if not os.path.exists(TREND_JSON_PATH):
        print("⚠ Sentiment data not found")
        return {}

    with open(TREND_JSON_PATH, "r") as f:
        data = json.load(f)

    sentiment = {}

    for bank, info in data.items():

        sentiment[bank] = {
            int(year): score
            for year, score in info["yearly_sentiment"].items()
        }

    return sentiment


# ==========================================
# LOAD STOCK RETURNS FROM SQLITE
# ==========================================

def load_stock_returns():

    if not os.path.exists(DB_PATH):
        print("⚠ Database not found:", DB_PATH)
        return {}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:

        cursor.execute("""
            SELECT bank_name, year, return
            FROM stock_returns
        """)

        rows = cursor.fetchall()

    except Exception:
        print("⚠ stock_returns table not found in database")
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
# INTERPRETATION LOGIC
# ==========================================

def interpret(sentiment_score, stock_return):

    sentiment = sentiment_label(sentiment_score)

    if sentiment == "Positive" and stock_return > 0:
        return "Customer perception and investor confidence are aligned."

    elif sentiment == "Negative" and stock_return > 0:
        return "Investors remain confident despite negative customer sentiment."

    elif sentiment == "Positive" and stock_return < 0:
        return "Positive customer perception not reflected in market performance."

    elif sentiment == "Neutral" and stock_return > 0:
        return "Market performance improved while customer sentiment remained neutral."

    elif sentiment == "Neutral" and stock_return < 0:
        return "Market declined despite neutral customer sentiment."

    else:
        return "Operational challenges reflected in both sentiment and stock returns."


# ==========================================
# MAIN ENGINE
# ==========================================

def main():

    print("\n📊 Running Strategic Market Intelligence...\n")

    sentiment_data = load_sentiment_data()
    stock_data = load_stock_returns()

    report = []

    report.append("STRATEGIC MARKET INTELLIGENCE REPORT")
    report.append("====================================\n")

    for bank in sorted(sentiment_data.keys()):

        report.append(f"\n🏦 {bank}")
        report.append("-" * (len(bank) + 3))

        sentiment = sentiment_data.get(bank, {})
        stock = stock_data.get(bank, {})

        years = sorted(set(sentiment.keys()) | set(stock.keys()))

        for year in years:

            report.append(f"\n📅 {year}")

            # ==========================
            # SENTIMENT
            # ==========================

            if year in sentiment:

                s = sentiment[year]

                mood = sentiment_label(s)

                report.append(
                    f"Customer Sentiment: {s:.3f} ({mood})"
                )

            else:

                report.append("Customer Sentiment: Not available")

            # ==========================
            # STOCK RETURN
            # ==========================

            if year in stock:

                r = stock[year]

                direction = "Positive" if r > 0 else "Negative"

                report.append(
                    f"Stock Market Return: {r:.3f} ({direction})"
                )

            else:

                report.append("Stock Market Return: Not available")

            # ==========================
            # INTERPRETATION
            # ==========================

            if year in sentiment and year in stock:

                explanation = interpret(
                    sentiment[year],
                    stock[year]
                )

                report.append(f"Interpretation: {explanation}")

        report.append("\n" + "=" * 60)

    final_text = "\n".join(report)

    with open(OUTPUT_PATH, "w") as f:
        f.write(final_text)

    print("\n✅ Strategic intelligence report generated")
    print("📄 Saved to:", OUTPUT_PATH)


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":
    main()