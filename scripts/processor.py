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

    def process(self, texts, ratings=None):
        # ratings: optional list same len as texts; 1-5 scale, can be None per item

        # ==============================
        # SAFETY CHECK: Empty Input
        # ==============================
        if len(texts) == 0:
            print("⚠ No texts found. Exiting.")
            return [], "", {
                "total_reviews": 0,
                "overall_sentiment": 0.0,
                "overall_rating": None,
            }

        if ratings is not None and len(ratings) != len(texts):
            ratings = None

        sentiments = self.sentiment_model.predict_batch(texts)
        embeddings = self.embedding_model.encode(texts)
        print("sentiment: ", sentiments)
        
        # ==============================
        # SAFE CLUSTERING FIX
        # ==============================
        if len(texts) < self.topic_model.model.n_clusters:
            topics = [0] * len(texts)
        else:
            topics = self.topic_model.fit_predict(embeddings)

        def norm_rating(r):
            if r is None or (isinstance(r, float) and np.isnan(r)):
                return None
            try:
                x = float(r)
                return (x - 3.0) / 2.0 if 1 <= x <= 5 else None
            except (TypeError, ValueError):
                return None

        results = []
        cluster_data = defaultdict(list)
        cluster_texts = defaultdict(list)
        rating_values = []

        for i, (text, sentiment) in enumerate(zip(texts, sentiments)):

            cluster_id = int(topics[i])
            score = sentiment["score"]
            label = sentiment["label"]

            sentiment_value = score if label == "POSITIVE" else -score

            row = {
                "text": text,
                "sentiment": label,
                "confidence": score,
                "topic_cluster": cluster_id
            }
            if ratings is not None:
                raw = ratings[i] if i < len(ratings) else None
                n = norm_rating(raw)
                if raw is not None:
                    row["rating"] = raw
                if n is not None:
                    row["rating_normalized"] = n
                    rating_values.append(n)
            results.append(row)

            cluster_data[cluster_id].append(sentiment_value)
            cluster_texts[cluster_id].append(text)

        # ==============================
        # EXECUTIVE SUMMARY
        # ==============================

        total_reviews = len(texts)
        overall_sentiment = np.mean(
            [v for values in cluster_data.values() for v in values]
        )

        summary_lines = []
        summary_lines.append("EXECUTIVE CUSTOMER INTELLIGENCE REPORT")
        summary_lines.append("=====================================\n")
        summary_lines.append(f"Total Reviews Analyzed: {total_reviews}")
        summary_lines.append(f"Overall Sentiment Score (from text): {overall_sentiment:.3f}")
        if rating_values:
            overall_rating = np.mean(rating_values)
            summary_lines.append(f"Overall Rating Score (from stars, -1 to 1): {overall_rating:.3f}")
        summary_lines.append("")

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

            # ==============================
            # SAFE KEYWORD EXTRACTION
            # ==============================
            if len(cluster_texts[cluster_id]) >= 2:
                vectorizer = TfidfVectorizer(
                    stop_words="english",
                    max_features=20
                )
                tfidf_matrix = vectorizer.fit_transform(
                    cluster_texts[cluster_id]
                )
                feature_names = vectorizer.get_feature_names_out()

                mean_scores = np.mean(tfidf_matrix.toarray(), axis=0)
                top_indices = mean_scores.argsort()[-8:][::-1]
                top_keywords = [feature_names[i] for i in top_indices]

                print("Top Keywords:", ", ".join(top_keywords))
                summary_lines.append(
                    "- Key Themes: " + ", ".join(top_keywords)
                )
            else:
                summary_lines.append("- Key Themes: Insufficient data")

        # ==============================
        # STRATEGIC INSIGHT
        # ==============================

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

        # ==============================
        # BENCHMARK METRICS
        # ==============================

        benchmark_data = {
            "total_reviews": total_reviews,
            "overall_sentiment": float(overall_sentiment),
            "overall_rating": float(np.mean(rating_values)) if rating_values else None,
        }

        return results, executive_summary, benchmark_data