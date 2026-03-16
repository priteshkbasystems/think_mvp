import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TopicSentimentCorrelation:

    def __init__(self):
        print("Loading Topic Sentiment Correlation Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        corp = pd.read_sql("""
        SELECT bank_name, topic, sentiment
        FROM corporate_topic_sentiment
        """, conn)

        cust = pd.read_sql("""
        SELECT bank_name, topic_id AS topic, AVG(sentiment_score) AS sentiment
        FROM review_sentiments
        GROUP BY bank_name, topic_id
        """, conn)

        merged = pd.merge(corp, cust, on=["bank_name","topic"], suffixes=("_corp","_cust"))

        correlations = {}

        for bank in merged["bank_name"].unique():

            df = merged[merged["bank_name"] == bank]

            if len(df) < 2:
                continue

            corr = df["sentiment_corp"].corr(df["sentiment_cust"])

            correlations[bank] = round(float(corr),3)

        conn.close()

        return correlations