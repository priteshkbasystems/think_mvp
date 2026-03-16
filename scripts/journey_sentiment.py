import sqlite3
import numpy as np

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


journey_keywords = {
    "onboarding": ["signup","register","account setup","verification"],
    "usage": ["app","transfer","payment","transaction"],
    "support": ["support","helpdesk","customer service","agent"],
    "billing": ["charge","fee","pricing","cost"]
}


class JourneySentiment:

    def __init__(self):
        print("Loading Journey Sentiment Engine")

    def detect_stage(self, text):

        text = text.lower()

        for stage, keywords in journey_keywords.items():

            for k in keywords:
                if k in text:
                    return stage

        return "other"

    def compute(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT review_text, sentiment_score
        FROM review_sentiments
        """)

        rows = cursor.fetchall()

        stages = {}

        for text, score in rows:

            stage = self.detect_stage(text)

            stages.setdefault(stage, []).append(score)

        results = {}

        for stage, scores in stages.items():
            results[stage] = round(float(np.mean(scores)),3)

        conn.close()

        return results