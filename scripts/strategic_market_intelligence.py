import os
import json
import sqlite3

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

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT bank_name, year, return
        FROM stock_returns
    """)

    rows = cursor.fetchall()

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

def interpret(sentiment, stock):

    if sentiment > 0 and stock > 0:
        return "Customer perception and investor confidence are aligned."

    elif sentiment < 0 and stock > 0:
        return "Investors remain confident despite negative customer sentiment."

    elif sentiment > 0 and stock < 0:
        return "Positive customer perception not reflected in market performance."

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

    for bank in sentiment_data:

        report.append(f"\n🏦 {bank}")
        report.append("-" * (len(bank) + 3))

        sentiment = sentiment_data.get(bank, {})
        stock = stock_data.get(bank, {})

        years = sorted(set(sentiment.keys()) | set(stock.keys()))

        for year in years:

            report.append(f"\n📅 {year}")

            if year in sentiment:

                s = sentiment[year]

                mood = "Positive" if s > 0 else "Negative"

                report.append(
                    f"Customer Sentiment: {s:.3f} ({mood})"
                )

            else:
                report.append("Customer Sentiment: Not available")

            if year in stock:

                r = stock[year]

                direction = "Positive" if r > 0 else "Negative"

                report.append(
                    f"Stock Market Return: {r:.3f} ({direction})"
                )

            else:
                report.append("Stock Market Return: Not available")

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