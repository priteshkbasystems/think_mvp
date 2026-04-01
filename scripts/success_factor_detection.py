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
            bank_id,
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
            bank_id,
            bank_name,
            AVG(sentiment_score) AS sentiment
        FROM review_sentiments
        GROUP BY bank_id, bank_name
        """, conn)

        conn.close()

        # -----------------------------------------
        # HANDLE EMPTY
        # -----------------------------------------
        if topics.empty or sentiments.empty:
            print("⚠ No topic sentiment data found")

            return pd.DataFrame({
                "bank_name": [],
                "bank_id": [],
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
            on=["bank_id", "bank_name"],
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
                "bank_id": [],
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

        top = merged.head(20)
        rows_to_save = []
        for _, row in top.iterrows():
            rows_to_save.append(
                (
                    int(row["bank_id"]),
                    row["bank_name"],
                    int(row["topic_id"]),
                    row["keywords"],
                    float(row["sentiment"]),
                    int(row["volume"]),
                )
            )

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO success_factors
            (bank_id, bank_name, topic_id, keywords, sentiment, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            rows_to_save,
        )
        conn.commit()
        conn.close()

        print("\nTop Transformation Success Factors:\n")

        return top