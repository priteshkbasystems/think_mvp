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
            return [], ""

        sentiments = self.sentiment_model.predict_batch(texts)
        embeddings = self.embedding_model.encode(texts)
        topics = self.topic_model.fit_predict(embeddings)

        results = []
        cluster_data = defaultdict(list)
        cluster_texts = defaultdict(list)

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

        # ==============================
        # Build Executive Summary
        # ==============================

        total_reviews = len(texts)
        overall_sentiment = np.mean([v for values in cluster_data.values() for v in values])

        summary_lines = []
        summary_lines.append("EXECUTIVE CUSTOMER INTELLIGENCE REPORT")
        summary_lines.append("=====================================\n")
        summary_lines.append(f"Total Reviews Analyzed: {total_reviews}")
        summary_lines.append(f"Overall Sentiment Score: {overall_sentiment:.3f}\n")

        print("\n📊 Cluster Summary:\n")

        for cluster_id, values in cluster_data.items():
            avg_sentiment = np.mean(values)
            count = len(values)
            percentage = (count / total_reviews) * 100

            print(f"Cluster {cluster_id}")
            print(f"Count: {count} ({percentage:.1f}%)")
            print(f"Avg Sentiment: {avg_sentiment:.3f}")

            summary_lines.append(f"\nCluster {cluster_id}")
            summary_lines.append(f"- Volume: {count} reviews ({percentage:.1f}%)")
            summary_lines.append(f"- Average Sentiment: {avg_sentiment:.3f}")

            # Keyword Extraction
            vectorizer = TfidfVectorizer(stop_words="english", max_features=20)
            tfidf_matrix = vectorizer.fit_transform(cluster_texts[cluster_id])
            feature_names = vectorizer.get_feature_names_out()
            mean_scores = np.mean(tfidf_matrix.toarray(), axis=0)
            top_indices = mean_scores.argsort()[-8:][::-1]
            top_keywords = [feature_names[i] for i in top_indices]

            print("Top Keywords:", ", ".join(top_keywords))
            summary_lines.append("- Key Themes: " + ", ".join(top_keywords))

        # Strategic Recommendation
        summary_lines.append("\nSTRATEGIC INSIGHT")
        summary_lines.append("-----------------")

        if overall_sentiment < -0.5:
            summary_lines.append(
                "Customer sentiment is strongly negative. Immediate investigation into technical stability and user experience is recommended."
            )
        elif overall_sentiment < 0:
            summary_lines.append(
                "Customer sentiment is mildly negative. Performance and usability improvements should be prioritized."
            )
        else:
            summary_lines.append(
                "Customer sentiment is generally positive. Focus on sustaining service quality and feature enhancement."
            )

        executive_summary = "\n".join(summary_lines)

        print("\n📈 Executive Insight Generated.\n")

        return results, executive_summary