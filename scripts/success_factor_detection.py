import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class SuccessFactorDetection:

    def __init__(self):
        print("Loading Success Factor Detection Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        # -----------------------------------------
        # CUSTOMER TOPICS (FROM complaint_topics)
        # -----------------------------------------
        topics = pd.read_sql("""
        SELECT 
            bank_name,
            topic_id,
            keywords,
            review_count
        FROM complaint_topics
        """, conn)

        # -----------------------------------------
        # CUSTOMER SENTIMENT (OVERALL)
        # -----------------------------------------
        sentiments = pd.read_sql("""
        SELECT 
            bank_name,
            AVG(sentiment_score) AS sentiment
        FROM review_sentiments
        GROUP BY bank_name
        """, conn)

        conn.close()

        # -----------------------------------------
        # HANDLE EMPTY
        # -----------------------------------------
        if topics.empty or sentiments.empty:
            print("⚠ No topic sentiment data found")

            return pd.DataFrame({
                "bank_name": [],
                "topic_id": [],
                "keywords": [],
                "sentiment": [],
                "volume": []
            })

        # -----------------------------------------
        # MERGE (APPROX — same sentiment per bank)
        # -----------------------------------------
        merged = pd.merge(
            topics,
            sentiments,
            on="bank_name",
            how="left"
        )

        # -----------------------------------------
        # CLEAN DATA
        # -----------------------------------------
        merged["review_count"] = merged["review_count"].fillna(0)

        # Rename for consistency
        merged = merged.rename(columns={
            "review_count": "volume"
        })

        # -----------------------------------------
        # FILTER LOW VOLUME
        # -----------------------------------------
        merged = merged[merged["volume"] >= 5]

        if merged.empty:
            print("⚠ No high-volume topics found")

            return pd.DataFrame({
                "bank_name": [],
                "topic_id": [],
                "keywords": [],
                "sentiment": [],
                "volume": []
            })

        # -----------------------------------------
        # SORT (SUCCESS FACTORS)
        # -----------------------------------------
        merged = merged.sort_values(
            ["bank_name", "sentiment"],
            ascending=[True, False]
        )

        print("\nTop Transformation Success Factors:\n")

        return merged.head(20)