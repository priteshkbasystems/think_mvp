import sqlite3
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TransformationImpactScore:

    def __init__(self):

        print("Loading Transformation Impact Score Engine...")

    def calculate_tis(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT bank_name FROM narrative_scores")
        banks = [row[0] for row in cursor.fetchall()]

        results = {}

        for bank in banks:

            cursor.execute("""
            SELECT year, score
            FROM narrative_scores
            WHERE bank_name=?
            ORDER BY year
            """, (bank,))

            narrative = cursor.fetchall()

            cursor.execute("""
            SELECT year, sentiment
            FROM sentiment_scores
            WHERE bank_name=?
            ORDER BY year
            """, (bank,))

            sentiment = cursor.fetchall()

            narrative_dict = {y: s for y, s in narrative}
            sentiment_dict = {y: s for y, s in sentiment}

            years = sorted(set(narrative_dict) & set(sentiment_dict))

            if len(years) < 2:
                continue

            narrative_values = [narrative_dict[y] for y in years]
            sentiment_values = [sentiment_dict[y] for y in years]

            delta_narrative = narrative_values[-1] - narrative_values[0]
            delta_sentiment = sentiment_values[-1] - sentiment_values[0]

            if delta_narrative == 0:
                tis = 0
            else:
                tis = delta_sentiment / delta_narrative

            results[bank] = round(tis, 3)

        conn.close()

        return results