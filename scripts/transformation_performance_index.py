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
        SELECT bank_id, AVG(score)
        FROM narrative_scores
        GROUP BY bank_id
        """)
        narrative = dict(cursor.fetchall())

        cursor.execute("""
        SELECT bank_id, AVG(sentiment)
        FROM sentiment_scores
        GROUP BY bank_id
        """)
        sentiment = dict(cursor.fetchall())

        cursor.execute("""
        SELECT bank_id, AVG(return)
        FROM stock_returns
        GROUP BY bank_id
        """)
        returns = dict(cursor.fetchall())

        cursor.execute("SELECT bank_id, bank_name FROM banks")
        bank_lookup = dict(cursor.fetchall())

        index = {}

        for bank in narrative:

            n = narrative.get(bank,0)
            s = sentiment.get(bank,0)
            r = returns.get(bank,0)

            score = np.mean([n,s,r])

            score_rounded = round(float(score),3)
            bank_name = bank_lookup.get(bank, str(bank))
            index[bank_name] = score_rounded
            cursor.execute(
                """
                INSERT OR REPLACE INTO transformation_performance_index
                (bank_id, bank_name, score, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (bank, bank_name, score_rounded),
            )

        conn.commit()
        conn.close()

        return index