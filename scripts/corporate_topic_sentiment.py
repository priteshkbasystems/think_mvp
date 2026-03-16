import re
import numpy as np
from collections import defaultdict

from sentence_transformers import SentenceTransformer
from models.sentiment_model import SentimentModel


class CorporateTopicSentiment:

    def __init__(self):

        print("Loading Corporate Topic Sentiment Engine...")

        self.sentiment_model = SentimentModel()

        self.embedder = SentenceTransformer("all-MiniLM-L6-v2")

        # transformation themes
        self.topics = [
            "digital transformation",
            "mobile banking app",
            "customer service",
            "ai automation",
            "payment systems",
            "security",
            "user experience",
            "operations efficiency",
            "cloud infrastructure",
            "data analytics"
        ]

        self.topic_embeddings = self.embedder.encode(self.topics)

    def split_sentences(self, text):

        sentences = re.split(r"[.!?]", text)

        sentences = [s.strip() for s in sentences if len(s.strip()) > 15]

        return sentences

    def classify_topic(self, sentence):

        emb = self.embedder.encode([sentence])[0]

        scores = np.dot(self.topic_embeddings, emb)

        idx = np.argmax(scores)

        return self.topics[idx]

    def analyze(self, text):

        sentences = self.split_sentences(text)

        if len(sentences) == 0:
            return {}

        sentiments = self.sentiment_model.predict_batch(sentences)

        topic_scores = defaultdict(list)

        for sentence, sentiment in zip(sentences, sentiments):

            topic = self.classify_topic(sentence)

            label = sentiment["label"]
            score = sentiment["score"]

            if label == "NEGATIVE":
                score = -score

            topic_scores[topic].append(score)

        results = {}

        for topic, scores in topic_scores.items():

            results[topic] = round(float(np.mean(scores)), 3)

        return results