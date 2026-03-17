import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TopicSentimentCorrelation:

    def __init__(self):
        print("Loading Topic Sentiment Correlation Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        # -----------------------------------------
        # CORPORATE TOPICS
        # -----------------------------------------
        corp = pd.read_sql("""
        SELECT bank_name, topic, AVG(sentiment) as sentiment
        FROM corporate_topic_sentiment
        GROUP BY bank_name, topic
        """, conn)

        # -----------------------------------------
        # CUSTOMER TOPICS (NO topic_id in reviews)
        # -----------------------------------------
        cust = pd.read_sql("""
        SELECT bank_name, topic_id, keywords
        FROM complaint_topics
        """, conn)

        conn.close()

        correlations = {}

        # -----------------------------------------
        # APPROXIMATE MATCH USING KEYWORDS
        # -----------------------------------------
        for bank in corp["bank_name"].unique():

            corp_df = corp[corp["bank_name"] == bank]
            cust_df = cust[cust["bank_name"] == bank]

            if len(corp_df) == 0 or len(cust_df) == 0:
                continue

            matches = []

            for _, c_row in corp_df.iterrows():

                topic = c_row["topic"].lower()

                for _, cust_row in cust_df.iterrows():

                    keywords = str(cust_row["keywords"]).lower()

                    # simple keyword overlap match
                    if topic in keywords or any(k in topic for k in keywords.split(",")):

                        matches.append({
                            "corp": c_row["sentiment"],
                            "cust": 0  # ⚠ we don't have customer sentiment per topic yet
                        })

            if len(matches) < 2:
                continue

            df = pd.DataFrame(matches)

            corr = df["corp"].corr(df["cust"])

            correlations[bank] = round(float(corr), 3) if pd.notna(corr) else 0

        return correlations