import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class SuccessFactorDetection:

    def __init__(self):

        print("Loading Success Factor Detection Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        topics = pd.read_sql("""
        SELECT topic_id, AVG(sentiment_score) AS sentiment
        FROM review_sentiments
        GROUP BY topic_id
        """, conn)

        topics = topics.sort_values("sentiment", ascending=False)

        conn.close()

        return topics.head(10)