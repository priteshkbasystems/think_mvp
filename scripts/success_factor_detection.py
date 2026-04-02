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
        topics = pd.read_sql(
            """
            SELECT
                bank_id,
                bank_name,
                topic_id,
                keywords,
                review_count
            FROM complaint_topics
            """,
            conn,
        )

        # -----------------------------------------
        # CUSTOMER SENTIMENT (TOPIC LEVEL)
        # -----------------------------------------
        sentiments = pd.read_sql(
            """
            SELECT
                bank_id,
                bank_name,
                topic_id,
                AVG(sentiment_score) AS sentiment,
                COUNT(*) AS sentiment_n
            FROM review_sentiments
            GROUP BY bank_id, bank_name, topic_id
            """,
            conn,
        )

        conn.close()

        # -----------------------------------------
        # HANDLE EMPTY
        # -----------------------------------------
        if topics.empty or sentiments.empty:
            print("⚠ No topic sentiment data found")
            return pd.DataFrame(
                {
                    "bank_name": [],
                    "bank_id": [],
                    "topic_id": [],
                    "keywords": [],
                    "sentiment": [],
                    "volume": [],
                    "sentiment_n": [],
                }
            )

        # -----------------------------------------
        # MERGE ON BANK + TOPIC
        # -----------------------------------------
        merged = pd.merge(
            topics,
            sentiments,
            on=["bank_id", "bank_name", "topic_id"],
            how="left",
        )

        # -----------------------------------------
        # CLEAN DATA
        # -----------------------------------------
        merged["review_count"] = merged["review_count"].fillna(0)
        merged["sentiment"] = merged["sentiment"].fillna(0)
        merged["sentiment_n"] = merged["sentiment_n"].fillna(0)

        merged = merged.rename(columns={"review_count": "volume"})

        # -----------------------------------------
        # FILTER LOW SIGNAL
        # -----------------------------------------
        merged = merged[(merged["volume"] >= 5) & (merged["sentiment_n"] >= 5)]

        if merged.empty:
            print("⚠ No high-volume topics found")
            return pd.DataFrame(
                {
                    "bank_name": [],
                    "bank_id": [],
                    "topic_id": [],
                    "keywords": [],
                    "sentiment": [],
                    "volume": [],
                    "sentiment_n": [],
                }
            )

        # Optional: keep only positive "success factors"
        # merged = merged[merged["sentiment"] > 0]

        # -----------------------------------------
        # SORT + TOP N PER BANK
        # -----------------------------------------
        merged = merged.sort_values(
            ["bank_name", "sentiment", "volume"],
            ascending=[True, False, False],
        )

        top = merged.groupby("bank_name", group_keys=False).head(5)

        rows_to_save = []
        for _, row in top.iterrows():
            rows_to_save.append(
                (
                    int(row["bank_id"]),
                    row["bank_name"],
                    row["topic_id"],
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