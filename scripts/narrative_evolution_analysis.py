import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class NarrativeEvolutionAnalysis:

    def __init__(self):

        print("Loading Narrative Evolution Analysis Engine")

    def compute(self):

        conn = sqlite3.connect(DB_PATH)

        df = pd.read_sql("""
        SELECT bank_name, year, score
        FROM narrative_scores
        """, conn)

        conn.close()

        pivot = df.pivot(index="bank_name", columns="year", values="score")

        trends = {}

        for bank in pivot.index:

            series = pivot.loc[bank].dropna()

            if len(series) < 2:
                continue

            trend = series.iloc[-1] - series.iloc[0]

            trends[bank] = round(float(trend),3)

        return pivot, trends