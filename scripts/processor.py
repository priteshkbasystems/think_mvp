from models.sentiment_model import SentimentModel
from models.embedding_model import EmbeddingModel
from models.topic_model import TopicModel

from collections import defaultdict
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer


class TextProcessor:
    def __init__(self):
        self.sentiment_model = SentimentModel()
        self.embedding_model = EmbeddingModel()
        self.topic_model = TopicModel(n_clusters=2)

    def process(self, texts):

        if len(texts) == 0:
            print("⚠ No texts found. Exiting.")
            return []

        sentiments = self.sentiment_model.predict_batch(texts)
        embeddings = self.embedding_model.encode(texts)
        topics = self.topic_model.fit_predict(embeddings)

        results = []
        cluster_data = defaultdict(list)
        cluster_texts = defaultdict(list)

        # ==========================
        # Collect Results
        # ==========================

        for i, (text, sentiment) in enumerate(zip(texts, sentiments)):
            cluster_id = int(topics[i])

            score = sentiment["score"]
            label = sentiment["label"]

            sentiment_value = score if label == "POSITIVE" else -score

            results.append({
                "text": text,
                "sentiment": label,
                "confidence": score,
                "topic_cluster": cluster_id
            })

            cluster_data[cluster_id].append(sentiment_value)
            cluster_texts[cluster_id].append(text)

        # ==========================
        # Cluster Summary
        # ==========================

        print("\n📊 Cluster Summary:")

        for cluster_id, values in cluster_data.items():
            avg_sentiment = np.mean(values)

            print(f"\nCluster {cluster_id}")
            print(f"Count: {len(values)}")
            print(f"Avg Sentiment: {avg_sentiment:.3f}")

            # ==========================
            # Keyword Extraction
            # ==========================

            texts_in_cluster = cluster_texts[cluster_id]

            vectorizer = TfidfVectorizer(
                stop_words="english",
                max_features=20
            )

            try:
                tfidf_matrix = vectorizer.fit_transform(texts_in_cluster)
                feature_names = vectorizer.get_feature_names_out()

                # Get average tfidf score for each word
                mean_scores = np.mean(tfidf_matrix.toarray(), axis=0)

                # Get top 10 words
                top_indices = mean_scores.argsort()[-10:][::-1]
                top_keywords = [feature_names[i] for i in top_indices]

                print("Top Keywords:", ", ".join(top_keywords))

            except Exception as e:
                print("Keyword extraction failed:", e)

        return results