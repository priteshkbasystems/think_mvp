from collections import Counter
import numbers
import os
from datetime import datetime

from scripts.utils.sentiment_utils import sentiment_label


class RootCauseAnalyzer:

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

            if "sentiment" in sentiment and "confidence" in sentiment:
                label = str(sentiment.get("sentiment", "")).upper()
                confidence = float(sentiment.get("confidence", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                return 0.0

            if "label" in sentiment and "score" in sentiment:
                label = str(sentiment.get("label", "")).upper()
                confidence = float(sentiment.get("score", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                return 0.0

            if "negative" in sentiment:
                return -float(sentiment.get("negative", 0))

            if "positive" in sentiment:
                return float(sentiment.get("positive", 0))

        try:
            return float(sentiment)
        except Exception:
            return 0.0

    # --------------------------------------------------
    # ROOT CAUSE ANALYSIS
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

        # Keyword groups
        keyword_map = {
            "Performance Issues": ["slow", "delay", "lag", "sluggish", "loading"],
            "App Crashes": ["crash", "freeze", "hang", "stuck"],
            "Login Problems": ["login", "otp", "authentication", "verify", "password"],
            "App Update Issues": ["update", "latest version"],
            "Payment Failures": ["payment", "transfer", "transaction", "qr", "bill"],
            "Accessibility / UX Issues": ["accessibility", "dark mode", "developer option", "usability"]
        }

        for text, sentiment in zip(texts, sentiments):

            score = self._extract_score(sentiment)
            label = sentiment_label(score)

            if label == "Negative":

                negative_count += 1
                text_lower = text.lower()

                matched = False

                for category, keywords in keyword_map.items():
                    if any(word in text_lower for word in keywords):
                        root_causes[category] += 1
                        matched = True
                        break

                if not matched:
                    root_causes["Other Complaints"] += 1

            elif label == "Neutral":
                neutral_count += 1

            else:
                positive_count += 1

        # --------------------------------------------------
        # BUILD REPORT
        # --------------------------------------------------

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

        report_lines.append("=" * 45)
        report_lines.append("")

        final_report = "\n".join(report_lines)

        # --------------------------------------------------
        # PRINT
        # --------------------------------------------------

        if verbose:
            print("\n" + final_report)

        # --------------------------------------------------
        # SAVE FILE
        # --------------------------------------------------

        if save_to_file:

            if output_dir is None:
                output_dir = "."

            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            if bank_name:
                filename = f"{bank_name}_root_cause_{timestamp}.txt"
            else:
                filename = f"root_cause_{timestamp}.txt"

            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(final_report)

            if verbose:
                print(f"📄 Root cause report saved to: {filepath}")

        return root_causes