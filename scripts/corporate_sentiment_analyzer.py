import re
import numpy as np
from models.sentiment_model import SentimentModel
from sentence_transformers import SentenceTransformer


class CorporateSentimentAnalyzer:

    def __init__(self):

        print("Loading Corporate Sentiment Analyzer...")

        self.sentiment_model = SentimentModel()
        self.embedder = SentenceTransformer("all-MiniLM-L6-v2")

        # corporate transformation themes
        self.topics = [
            "digital transformation",
            "mobile banking app",
            "customer service",
            "ai automation",
            "payment systems",
            "security",
            "user experience",
            "operations efficiency",
        ]

        self.topic_embeddings = self.embedder.encode(self.topics)

    def split_sentences(self, text):

        sentences = re.split(r'[.!?]', text)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 10]

        return sentences

    def classify_topic(self, sentence):

        emb = self.embedder.encode([sentence])[0]

        scores = np.dot(self.topic_embeddings, emb)

        best_idx = np.argmax(scores)

        return self.topics[best_idx]

    def analyze_document(self, text):

        sentences = self.split_sentences(text)

        if len(sentences) == 0:
            return {}

        sentiments = self.sentiment_model.predict_batch(sentences)

        topic_scores = {}

        for sent, sentiment in zip(sentences, sentiments):

            topic = self.classify_topic(sent)

            label = sentiment["label"]
            score = sentiment["score"]

            if label == "NEGATIVE":
                score = -score

            if topic not in topic_scores:
                topic_scores[topic] = []

            topic_scores[topic].append(score)

        results = {}

        for topic, scores in topic_scores.items():

            results[topic] = float(np.mean(scores))

        return results