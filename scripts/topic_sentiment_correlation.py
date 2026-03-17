import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TopicSentimentCorrelation:

    def __init__(self):
        print("Loading Topic Sentiment Correlation Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        # -----------------------------------------
        # CORPORATE TOPIC SENTIMENT
        # -----------------------------------------
        corp = pd.read_sql("""
        SELECT bank_name, topic, AVG(sentiment) AS sentiment
        FROM corporate_topic_sentiment
        GROUP BY bank_name, topic
        """, conn)

        # -----------------------------------------
        # CUSTOMER TOPIC SENTIMENT (NOW VALID)
        # -----------------------------------------
        cust = pd.read_sql("""
        SELECT bank_name, topic_id, AVG(sentiment_score) AS sentiment
        FROM review_sentiments
        WHERE topic_id IS NOT NULL
        GROUP BY bank_name, topic_id
        """, conn)

        # -----------------------------------------
        # TOPIC MAPPING (topic_id → keywords)
        # -----------------------------------------
        topics = pd.read_sql("""
        SELECT bank_name, topic_id, keywords
        FROM complaint_topics
        """, conn)

        conn.close()

        correlations = {}

        # -----------------------------------------
        # PROCESS PER BANK
        # -----------------------------------------
        for bank in corp["bank_name"].unique():

            corp_df = corp[corp["bank_name"] == bank]
            cust_df = cust[cust["bank_name"] == bank]
            topic_df = topics[topics["bank_name"] == bank]

            if corp_df.empty or cust_df.empty or topic_df.empty:
                continue

            merged_rows = []

            # -----------------------------------------
            # MATCH CORPORATE TOPIC ↔ CUSTOMER TOPIC
            # -----------------------------------------
            for _, c_row in corp_df.iterrows():

                corp_topic = str(c_row["topic"]).lower()
                corp_sentiment = c_row["sentiment"]

                for _, t_row in topic_df.iterrows():

                    topic_id = t_row["topic_id"]
                    keywords = str(t_row["keywords"]).lower()

                    # keyword-based matching
                    if corp_topic in keywords or any(k.strip() in corp_topic for k in keywords.split(",")):

                        cust_match = cust_df[cust_df["topic_id"] == topic_id]

                        if cust_match.empty:
                            continue

                        cust_sentiment = cust_match["sentiment"].values[0]

                        merged_rows.append({
                            "corp": corp_sentiment,
                            "cust": cust_sentiment
                        })

            if len(merged_rows) < 2:
                continue

            df = pd.DataFrame(merged_rows)

            corr = df["corp"].corr(df["cust"])

            if pd.notna(corr):
                correlations[bank] = round(float(corr), 3)

        return correlations