import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class SuccessFactorDetection:

    def __init__(self):
        print("Loading Success Factor Detection Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        # -----------------------------------------
        # CUSTOMER TOPIC SENTIMENT (VALID ONLY)
        # -----------------------------------------
        topics = pd.read_sql("""
        SELECT 
            bank_name,
            topic_id,
            AVG(sentiment_score) AS sentiment,
            COUNT(*) as volume
        FROM review_sentiments
        WHERE topic_id IS NOT NULL
        GROUP BY bank_name, topic_id
        """, conn)

        # -----------------------------------------
        # LOAD TOPIC NAMES
        # -----------------------------------------
        topic_names = pd.read_sql("""
        SELECT bank_name, topic_id, keywords
        FROM complaint_topics
        """, conn)

        conn.close()

        if topics.empty:
            print("⚠ No topic sentiment data found")
            return {}

        # -----------------------------------------
        # MERGE TO GET TOPIC LABELS
        # -----------------------------------------
        merged = pd.merge(
            topics,
            topic_names,
            on=["bank_name", "topic_id"],
            how="left"
        )

        # -----------------------------------------
        # FILTER LOW VOLUME (IMPORTANT)
        # -----------------------------------------
        merged = merged[merged["volume"] >= 5]

        results = {}

        # -----------------------------------------
        # TOP SUCCESS FACTORS PER BANK
        # -----------------------------------------
        for bank in merged["bank_name"].unique():

            df = merged[merged["bank_name"] == bank]

            if df.empty:
                continue

            df = df.sort_values("sentiment", ascending=False)

            top = df.head(5)

            results[bank] = [
                {
                    "topic_id": int(row["topic_id"]),
                    "keywords": row["keywords"],
                    "sentiment": round(float(row["sentiment"]), 3),
                    "volume": int(row["volume"])
                }
                for _, row in top.iterrows()
            ]

        return results