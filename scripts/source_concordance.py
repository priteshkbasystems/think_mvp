import sqlite3
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class SourceConcordance:

    def __init__(self):
        print("Loading Source Concordance Engine...")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT bank_id, bank_name, review_source, sentiment_score
        FROM review_sentiments
        """)

        rows = cursor.fetchall()

        bank_sources = {}

        for bank_id, bank, source, score in rows:

            if source is None:
                source = "unknown"

            bank_sources.setdefault((bank_id, bank), {})
            bank_sources[(bank_id, bank)].setdefault(source, [])
            bank_sources[(bank_id, bank)][source].append(score)

        results = {}

        for (bank_id, bank), sources in bank_sources.items():

            source_avg = {}

            for source, scores in sources.items():

                avg = float(np.mean(scores))

                source_avg[source] = avg

                cursor.execute("""
                INSERT INTO source_concordance
                (bank_id, bank_name, review_source, avg_sentiment)
                VALUES (?, ?, ?, ?)
                """, (bank_id, bank, source, avg))

            values = list(source_avg.values())

            if len(values) > 1:
                concordance = 1 - np.std(values)
            else:
                concordance = 1.0

            results[bank] = {
                "sources": source_avg,
                "concordance_score": round(float(concordance), 3)
            }

        conn.commit()
        conn.close()

        return results