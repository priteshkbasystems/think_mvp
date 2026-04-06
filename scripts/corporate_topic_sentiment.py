import re
import numpy as np
from collections import defaultdict

from models.sentiment_model import SentimentModel
from models.embedding_model import EmbeddingModel
from services.openai_service import OpenAIService, USE_OPENAI


class CorporateTopicSentiment:

    def __init__(self):

        print("Loading Corporate Topic Sentiment Engine...")

        self.sentiment_model = SentimentModel()
        self.use_openai = USE_OPENAI
        self.openai = OpenAIService() if self.use_openai else None
        self.embedder = None if self.use_openai else EmbeddingModel()

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

        self.topic_embeddings = None if self.use_openai else self.embedder.encode(self.topics)

    def split_sentences(self, text):

        sentences = re.split(r"[.!?]", text)

        sentences = [s.strip() for s in sentences if len(s.strip()) > 15]

        return sentences

    def classify_topic(self, sentence):
        if self.use_openai:
            return self.openai.topic_classification(sentence, candidates=self.topics)["topic"]

        emb = self.embedder.encode([sentence])[0]

        scores = np.dot(self.topic_embeddings, emb)

        idx = np.argmax(scores)

        return self.topics[idx]

    def analyze(self, text):

        sentences = self.split_sentences(text)

        if len(sentences) == 0:
            return {}

        sentiments = self.sentiment_model.predict_batch(sentences)
        topics = (
            [x["topic"] for x in self.openai.topic_classification_batch(sentences, self.topics)]
            if self.use_openai
            else [self.classify_topic(s) for s in sentences]
        )

        topic_scores = defaultdict(list)

        for sentence, sentiment, topic in zip(sentences, sentiments, topics):

            label = sentiment["label"]
            score = sentiment["score"]

            if label == "NEGATIVE":
                score = -score

            topic_scores[topic].append(score)

        results = {}

        for topic, scores in topic_scores.items():

            results[topic] = round(float(np.mean(scores)), 3)

        return results