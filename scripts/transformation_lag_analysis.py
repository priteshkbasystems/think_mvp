import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TransformationLagAnalysis:

    def __init__(self):

        print("Loading Transformation Lag Analysis Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        narrative = pd.read_sql("""
        SELECT bank_id, bank_name, year, score
        FROM narrative_scores
        """, conn)

        sentiment = pd.read_sql("""
        SELECT bank_id, bank_name, year, sentiment
        FROM sentiment_scores
        """, conn)

        conn.close()

        results = {}

        banks = narrative["bank_id"].unique()

        for bank in banks:

            n = narrative[narrative["bank_id"] == bank].sort_values("year")
            s = sentiment[sentiment["bank_id"] == bank].sort_values("year")

            merged = pd.merge(n, s, on=["bank_id", "bank_name", "year"])

            if len(merged) < 3:
                continue

            n_vals = merged["score"].values
            s_vals = merged["sentiment"].values

            best_lag = 0
            best_corr = -1

            for lag in range(3):

                if lag == 0:

                    corr = np.corrcoef(n_vals, s_vals)[0,1]

                else:

                    corr = np.corrcoef(n_vals[:-lag], s_vals[lag:])[0,1]

                if corr > best_corr:

                    best_corr = corr
                    best_lag = lag

            bank_label = merged["bank_name"].iloc[0]
            results[bank_label] = {
                "lag_years": best_lag,
                "correlation": round(float(best_corr),3)
            }

        return results