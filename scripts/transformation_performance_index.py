import sqlite3
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TransformationPerformanceIndex:

    def __init__(self):
        print("Loading Transformation Performance Index Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT bank_name, AVG(score)
        FROM narrative_scores
        GROUP BY bank_name
        """)
        narrative = dict(cursor.fetchall())

        cursor.execute("""
        SELECT bank_name, AVG(sentiment)
        FROM sentiment_scores
        GROUP BY bank_name
        """)
        sentiment = dict(cursor.fetchall())

        cursor.execute("""
        SELECT bank_name, AVG(return)
        FROM stock_returns
        GROUP BY bank_name
        """)
        returns = dict(cursor.fetchall())

        index = {}

        for bank in narrative:

            n = narrative.get(bank,0)
            s = sentiment.get(bank,0)
            r = returns.get(bank,0)

            score = np.mean([n,s,r])

            index[bank] = round(float(score),3)

        conn.close()

        return index