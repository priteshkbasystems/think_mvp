import sqlite3
import numpy as np
from services.openai_service import OpenAIService, USE_OPENAI

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
        self.use_openai = USE_OPENAI
        self.openai = OpenAIService() if self.use_openai else None

    def detect_stage(self, text):
        if self.use_openai:
            return self.openai.topic_classification(
                text,
                candidates=list(journey_keywords.keys()) + ["other"],
            )["topic"]

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
            value = score
            if self.use_openai:
                s = self.openai.sentiment(text)
                value = float(s["score"])
                if s["label"] == "NEGATIVE":
                    value = -value
                elif s["label"] == "NEUTRAL":
                    value = 0.0

            stages.setdefault(stage, []).append(value)

        results = {}

        for stage, scores in stages.items():
            value = round(float(np.mean(scores)),3)
            results[stage] = value
            cursor.execute(
                """
                INSERT OR REPLACE INTO journey_sentiment
                (stage, sentiment, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                (stage, value),
            )

        conn.commit()
        conn.close()

        return results