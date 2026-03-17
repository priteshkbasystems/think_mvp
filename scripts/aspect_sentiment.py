import numpy as np
from collections import defaultdict
from sentence_transformers import SentenceTransformer
from scripts.utils.sentiment_utils import analyze_sentiment
from models.sentiment_model import SentimentModel
import time


class AspectSentimentAnalyzer:

    def __init__(self):

        print("🚀 Loading Aspect Sentiment Analyzer...")

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

        # ✅ Precompute aspect embeddings once
        self.aspect_embeddings = self.embedder.encode(self.aspects)

        print("✅ Aspect model ready")

    # -----------------------------------------
    # Batch Aspect Classification
    # -----------------------------------------
    def classify_aspect_batch(self, texts):

        # ✅ Encode all texts in batch (FAST)
        text_embeddings = self.embedder.encode(texts, batch_size=128)

        # Cosine similarity via dot product
        scores = np.dot(text_embeddings, self.aspect_embeddings.T)

        # Get best matching aspect index
        indices = np.argmax(scores, axis=1)

        return [self.aspects[i] for i in indices]

    # -----------------------------------------
    # MAIN ANALYSIS (BATCH + LOGS)
    # -----------------------------------------
    def analyze(self, texts, ratings, batch_size=128):

        print("\n🔎 Starting Aspect Sentiment Analysis...")
        print(f"Total Reviews: {len(texts)}\n")

        start_time = time.time()

        aspect_scores = defaultdict(list)

        for i in range(0, len(texts), batch_size):

            batch_texts = texts[i:i+batch_size]
            batch_ratings = ratings[i:i+batch_size]

            # ✅ Batch sentiment prediction
            sentiments = self.sentiment_model.predict_batch(batch_texts)

            # ✅ Batch aspect classification
            aspects = self.classify_aspect_batch(batch_texts)

            for text, rating, sentiment, aspect in zip(
                batch_texts, batch_ratings, sentiments, aspects
            ):

                score = sentiment["score"]

                if sentiment["label"] == "NEGATIVE":
                    score = -score

                final_score, _ = analyze_sentiment(score, rating)

                aspect_scores[aspect].append(final_score)

            # ✅ Progress Logging
            processed = min(i + batch_size, len(texts))
            elapsed = round(time.time() - start_time, 2)

            print(f"✅ Processed {processed}/{len(texts)} | ⏱ {elapsed}s")

        # -----------------------------------------
        # Aggregate results
        # -----------------------------------------
        result = {}

        for aspect, scores in aspect_scores.items():
            result[aspect] = float(np.mean(scores))

        print("\n🎯 Aspect Sentiment Completed\n")

        return result