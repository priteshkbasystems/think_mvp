# models/sentiment_model.py

from transformers import pipeline

class SentimentModel:
    def __init__(self):
        self.model = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device=0
        )

    def predict(self, text: str):
        return self.model(text)