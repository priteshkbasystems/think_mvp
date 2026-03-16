import numpy as np
from sentence_transformers import SentenceTransformer


class CustomSentimentTaxonomy:

    def __init__(self):

        print("Loading Custom Sentiment Taxonomy Engine...")

        self.model = SentenceTransformer("all-MiniLM-L6-v2")

        # emotion taxonomy
        self.emotions = [
            "frustration",
            "delight",
            "confusion",
            "anger",
            "satisfaction",
            "trust"
        ]

        # business categories
        self.categories = [
            "ux friction",
            "pricing objection",
            "delivery delay",
            "login issues",
            "customer support",
            "mobile app performance",
            "security concerns",
            "payment problems"
        ]

        self.emotion_embeddings = self.model.encode(self.emotions)
        self.category_embeddings = self.model.encode(self.categories)

    def classify(self, text):

        emb = self.model.encode([text])[0]

        emotion_scores = np.dot(self.emotion_embeddings, emb)
        category_scores = np.dot(self.category_embeddings, emb)

        emotion = self.emotions[np.argmax(emotion_scores)]
        category = self.categories[np.argmax(category_scores)]

        return emotion, category