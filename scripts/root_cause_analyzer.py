from collections import Counter
import numbers
import os
from datetime import datetime

from scripts.utils.sentiment_utils import sentiment_label
from scripts.topic_discovery import ComplaintTopicDiscovery
from scripts.db_cache import save_complaint_topics

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


class RootCauseAnalyzer:

    def __init__(self):

        print("Loading root cause AI model...")

        self.model = SentenceTransformer("all-MiniLM-L6-v2")

        # Topic discovery engine
        self.topic_engine = ComplaintTopicDiscovery()

        self.root_cause_themes = {
            "Performance Issues": "app is slow lagging loading delay",
            "App Crashes": "application crashes freezes stuck stops working",
            "Login Problems": "cannot login authentication otp verification password problem",
            "App Update Issues": "problem after app update latest version issue",
            "Payment Failures": "payment transfer transaction failed qr bill payment issue",
            "Accessibility / UX Issues": "bad interface usability accessibility navigation ui ux"
        }

        self.theme_names = list(self.root_cause_themes.keys())

        self.theme_embeddings = self.model.encode(
            list(self.root_cause_themes.values())
        )


    # --------------------------------------------------
    # SENTIMENT NORMALIZATION
    # --------------------------------------------------

    def _extract_score(self, sentiment):

        while isinstance(sentiment, list) and len(sentiment) > 0:
            sentiment = sentiment[0]

        try:
            import torch
            if isinstance(sentiment, torch.Tensor):
                sentiment = sentiment.item()
        except Exception:
            pass

        if isinstance(sentiment, numbers.Number):
            return float(sentiment)

        if isinstance(sentiment, dict):

            if "label" in sentiment and "score" in sentiment:

                label = str(sentiment["label"]).upper()
                score = float(sentiment["score"])

                if label == "NEGATIVE":
                    return -score
                elif label == "POSITIVE":
                    return score

        try:
            return float(sentiment)
        except:
            return 0.0


    # --------------------------------------------------
    # AI ROOT CAUSE CLASSIFICATION
    # --------------------------------------------------

    def classify_root_cause(self, text):

        embedding = self.model.encode([text])

        similarities = cosine_similarity(
            embedding,
            self.theme_embeddings
        )[0]

        best_index = similarities.argmax()

        confidence = similarities[best_index]

        if confidence < 0.35:
            return "Other Complaints"

        return self.theme_names[best_index]


    # --------------------------------------------------
    # MAIN ANALYSIS
    # --------------------------------------------------

    def analyze(self, texts, sentiments,
                bank_name=None,
                save_to_file=False,
                output_dir=None,
                verbose=True):

        if not texts or not sentiments:
            return Counter()

        root_causes = Counter()

        negative_count = 0
        neutral_count = 0
        positive_count = 0

        negative_texts = []

        for text, sentiment in zip(texts, sentiments):

            score = self._extract_score(sentiment)

            label = sentiment_label(score)

            if label == "Negative":

                negative_count += 1

                negative_texts.append(text)

                cause = self.classify_root_cause(text)

                root_causes[cause] += 1

            elif label == "Neutral":
                neutral_count += 1

            else:
                positive_count += 1


        # -----------------------------------------
        # DISCOVER NEW COMPLAINT TOPICS
        # -----------------------------------------

        topics = {}

        if len(negative_texts) >= 5:
            try:
                topics = self.topic_engine.discover_topics(negative_texts)
                save_complaint_topics(bank_name, topics)
            except Exception:
                topics = {}


        # -----------------------------------------
        # REPORT
        # -----------------------------------------

        report_lines = []

        report_lines.append("🔍 ROOT CAUSE ANALYSIS")
        report_lines.append("=" * 45)

        if bank_name:
            report_lines.append(f"Bank: {bank_name}")

        total_reviews = len(sentiments)

        report_lines.append(f"Total Reviews: {total_reviews}")
        report_lines.append(f"Positive Reviews: {positive_count}")
        report_lines.append(f"Neutral Reviews: {neutral_count}")
        report_lines.append(f"Negative Reviews: {negative_count}")
        report_lines.append("")

        if negative_count == 0:

            report_lines.append("⚠ No negative reviews detected.")

        else:

            report_lines.append("Main Complaint Categories:\n")

            for cause, count in root_causes.most_common():

                percentage = (count / negative_count) * 100

                report_lines.append(
                    f"{cause}: {count} ({percentage:.1f}%)"
                )

        # -----------------------------------------
        # EMERGING TOPICS
        # -----------------------------------------

        if topics:

            report_lines.append("\n🔎 Emerging Complaint Topics:\n")

            for topic_id, keywords in topics.items():

                report_lines.append(
                    f"Topic {topic_id}: {', '.join(keywords)}"
                )

        report_lines.append("=" * 45)

        final_report = "\n".join(report_lines)

        if verbose:
            print("\n" + final_report)

        # -----------------------------------------
        # SAVE REPORT
        # -----------------------------------------

        if save_to_file:

            if output_dir is None:
                output_dir = "."

            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            filename = f"{bank_name}_root_cause_{timestamp}.txt"

            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(final_report)

            print(f"\n📄 Root cause report saved to: {filepath}")

        return root_causes