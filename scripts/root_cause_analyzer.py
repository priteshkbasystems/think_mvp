from collections import Counter
import numbers
import os
from datetime import datetime


class RootCauseAnalyzer:

    # --------------------------------------------------
    # SENTIMENT NORMALIZATION
    # --------------------------------------------------
    def _extract_score(self, sentiment):

        # Unwrap nested lists
        while isinstance(sentiment, list) and len(sentiment) > 0:
            sentiment = sentiment[0]

        # Torch tensor support
        try:
            import torch
            if isinstance(sentiment, torch.Tensor):
                sentiment = sentiment.item()
        except Exception:
            pass

        # Numeric
        if isinstance(sentiment, numbers.Number):
            return float(sentiment)

        # Dictionary formats
        if isinstance(sentiment, dict):

            # Custom pipeline format
            if "sentiment" in sentiment and "confidence" in sentiment:
                label = str(sentiment.get("sentiment", "")).upper()
                confidence = float(sentiment.get("confidence", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                return 0.0

            # HuggingFace format
            if "label" in sentiment and "score" in sentiment:
                label = str(sentiment.get("label", "")).upper()
                confidence = float(sentiment.get("score", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                return 0.0

            # Probability format
            if "negative" in sentiment:
                return -float(sentiment.get("negative", 0))

            if "positive" in sentiment:
                return float(sentiment.get("positive", 0))

        # Fallback
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

        root_causes = Counter()
        negative_count = 0

        for text, sentiment in zip(texts, sentiments):

            score = self._extract_score(sentiment)

            if score < 0:
                negative_count += 1
                text_lower = text.lower()

                if any(word in text_lower for word in
                       ["slow", "delay", "lag", "sluggish", "loading"]):
                    root_causes["Performance Issues"] += 1

                elif any(word in text_lower for word in
                         ["crash", "freeze", "hang", "stuck"]):
                    root_causes["App Crashes"] += 1

                elif any(word in text_lower for word in
                         ["login", "otp", "authentication", "verify", "password"]):
                    root_causes["Login Problems"] += 1

                elif "update" in text_lower:
                    root_causes["App Update Issues"] += 1

                elif any(word in text_lower for word in
                         ["payment", "transfer", "transaction", "qr", "bill"]):
                    root_causes["Payment Failures"] += 1

                elif any(word in text_lower for word in
                         ["accessibility", "dark mode", "developer option", "usability"]):
                    root_causes["Accessibility / UX Issues"] += 1

                else:
                    root_causes["Other Complaints"] += 1

        # --------------------------------------------------
        # BUILD REPORT STRING
        # --------------------------------------------------
        report_lines = []
        report_lines.append("🔍 ROOT CAUSE ANALYSIS")
        report_lines.append("=" * 40)

        if bank_name:
            report_lines.append(f"Bank: {bank_name}")

        report_lines.append(f"Total Reviews: {len(sentiments)}")
        report_lines.append(f"Negative Reviews: {negative_count}")
        report_lines.append("")

        if negative_count == 0:
            report_lines.append("⚠ No negative reviews detected.")
        else:
            for cause, count in root_causes.most_common():
                percentage = (count / negative_count) * 100
                report_lines.append(
                    f"{cause}: {count} ({percentage:.1f}%)"
                )

        report_lines.append("=" * 40)
        report_lines.append("")

        final_report = "\n".join(report_lines)

        # --------------------------------------------------
        # PRINT TO CONSOLE
        # --------------------------------------------------
        if verbose:
            print("\n" + final_report)

        # --------------------------------------------------
        # SAVE TO TXT FILE
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