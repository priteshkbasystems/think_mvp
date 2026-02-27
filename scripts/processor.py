from models.sentiment_model import SentimentModel
from models.embedding_model import EmbeddingModel
from models.topic_model import TopicModel

from collections import defaultdict
import numpy as np


class TextProcessor:
    def __init__(self):
        self.sentiment_model = SentimentModel()
        self.embedding_model = EmbeddingModel()
        self.topic_model = TopicModel(n_clusters=2)

    def process(self, texts):
        sentiments = self.sentiment_model.predict_batch(texts)
        embeddings = self.embedding_model.encode(texts)
        topics = self.topic_model.fit_predict(embeddings)

        results = []
        cluster_data = defaultdict(list)

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

        print("\n📊 Cluster Summary:")
        for cluster_id, values in cluster_data.items():
            avg_sentiment = np.mean(values)
            print(f"Cluster {cluster_id} → Count: {len(values)}, Avg Sentiment: {avg_sentiment:.3f}")

        return results