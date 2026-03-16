import numpy as np
from collections import defaultdict
from sentence_transformers import SentenceTransformer
from scripts.utils.sentiment_utils import analyze_sentiment
from models.sentiment_model import SentimentModel


class AspectSentimentAnalyzer:

    def __init__(self):

        print("Loading Aspect Sentiment Analyzer...")

        self.embedder = SentenceTransformer("all-MiniLM-L6-v2")
        self.sentiment_model = SentimentModel()

        self.aspects = [
            "mobile app",
            "customer service",
            "login",
            "payments",
            "security",
            "pricing",
            "user experience"
        ]

        self.aspect_embeddings = self.embedder.encode(self.aspects)

    def classify_aspect(self, text):

        emb = self.embedder.encode([text])[0]

        scores = np.dot(self.aspect_embeddings, emb)

        idx = np.argmax(scores)

        return self.aspects[idx]

    def analyze(self, texts, ratings):

        sentiments = self.sentiment_model.predict_batch(texts)

        aspect_scores = defaultdict(list)

        for text, rating, sentiment in zip(texts, ratings, sentiments):

            label = sentiment["label"]
            score = sentiment["score"]

            if label == "NEGATIVE":
                score = -score

            final_score, _ = analyze_sentiment(score, rating)

            aspect = self.classify_aspect(text)

            aspect_scores[aspect].append(final_score)

        result = {}

        for aspect, scores in aspect_scores.items():
            result[aspect] = float(np.mean(scores))

        return result