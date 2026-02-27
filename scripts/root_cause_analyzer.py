from collections import Counter
import numbers


class RootCauseAnalyzer:

    # --------------------------------------------------
    # SENTIMENT NORMALIZATION (FINAL FIX)
    # --------------------------------------------------
    def _extract_score(self, sentiment):
        """
        Convert any known sentiment format into signed numeric polarity.
        """

        original = sentiment

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

        # Numeric types
        if isinstance(sentiment, numbers.Number):
            return float(sentiment)

        # Dictionary formats
        if isinstance(sentiment, dict):

            # 🔥 YOUR CUSTOM FORMAT FIX
            # { "sentiment": "NEGATIVE", "confidence": 0.99 }
            if "sentiment" in sentiment and "confidence" in sentiment:
                label = str(sentiment.get("sentiment", "")).upper()
                confidence = float(sentiment.get("confidence", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                else:
                    return 0.0

            # HuggingFace format
            if "label" in sentiment and "score" in sentiment:
                label = str(sentiment.get("label", "")).upper()
                confidence = float(sentiment.get("score", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                else:
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
            print("⚠ Unknown sentiment format:", original)
            return 0.0

    # --------------------------------------------------
    # ROOT CAUSE ANALYSIS
    # --------------------------------------------------
    def analyze(self, texts, sentiments, verbose=True):

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
        # PRINT OUTPUT
        # --------------------------------------------------
        if verbose:

            print("\n🔍 ROOT CAUSE BREAKDOWN:")
            print(f"Total Reviews: {len(sentiments)}")
            print(f"Negative Reviews Detected: {negative_count}\n")

            if negative_count == 0:
                print("⚠ No negative reviews detected.\n")
            else:
                for cause, count in root_causes.most_common():
                    percentage = (count / negative_count) * 100
                    print(f"{cause}: {count} ({percentage:.1f}%)")

                print("\n📊 Categories Identified:",
                      len(root_causes))
                print("-" * 45)

        return root_causes