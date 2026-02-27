from collections import defaultdict
import numpy as np

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