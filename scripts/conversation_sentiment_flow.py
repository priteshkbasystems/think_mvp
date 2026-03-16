import sqlite3
import numpy as np
from models.sentiment_model import SentimentModel

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class ConversationSentimentFlow:

    def __init__(self):

        print("Loading Conversation Sentiment Engine")

        self.model = SentimentModel()

    def analyze_conversation(self, messages):

        sentiments = []

        preds = self.model.predict_batch(messages)

        for p in preds:

            score = p["score"]

            if p["label"] == "NEGATIVE":
                score = -score

            sentiments.append(score)

        escalation = np.mean(np.diff(sentiments)) if len(sentiments) > 1 else 0

        return sentiments, escalation

    def save(self, conversation_id, messages):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        sentiments, escalation = self.analyze_conversation(messages)

        for i, msg in enumerate(messages):

            cursor.execute("""
            INSERT INTO conversation_sentiment_flow
            (conversation_id, step, message, sentiment)
            VALUES (?, ?, ?, ?)
            """, (conversation_id, i, msg, sentiments[i]))

        conn.commit()
        conn.close()

        return escalation