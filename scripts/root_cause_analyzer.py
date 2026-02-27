from collections import Counter
import numbers


class RootCauseAnalyzer:
    """
    Enterprise Root Cause Analyzer for customer reviews.

    Supports:
        - Python int/float
        - numpy numeric types
        - torch tensors
        - HuggingFace nested list output
        - HuggingFace dict output
        - Custom probability format
    """

    # --------------------------------------------------
    # SENTIMENT NORMALIZATION (FULLY ROBUST)
    # --------------------------------------------------
    def _extract_score(self, sentiment):
        """
        Convert any sentiment format into signed numeric polarity.
        Always returns float.
        """

        # 🔹 1. Unwrap deeply nested lists (HF sometimes returns [[{...}]])
        while isinstance(sentiment, list) and len(sentiment) > 0:
            sentiment = sentiment[0]

        # 🔹 2. Torch tensor
        try:
            import torch
            if isinstance(sentiment, torch.Tensor):
                sentiment = sentiment.item()
        except Exception:
            pass

        # 🔹 3. Any numeric type (int, float, numpy.float32, etc.)
        if isinstance(sentiment, numbers.Number):
            return float(sentiment)

        # 🔹 4. Dictionary formats
        if isinstance(sentiment, dict):

            # HuggingFace format
            if "label" in sentiment:
                label = str(sentiment.get("label", "")).upper()
                confidence = float(sentiment.get("score", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                else:
                    return 0.0

            # Custom probability format
            if "negative" in sentiment:
                return -float(sentiment.get("negative", 0))

            if "positive" in sentiment:
                return float(sentiment.get("positive", 0))

        # 🔹 5. Final safe fallback
        try:
            return float(sentiment)
        except Exception:
            return 0.0

    # --------------------------------------------------
    # ROOT CAUSE ANALYSIS
    # --------------------------------------------------
    def analyze(self, texts, sentiments, verbose=True):
        """
        Analyze root causes from negative reviews.
        Returns Counter object.
        """

        root_causes = Counter()
        negative_count = 0

        for text, sentiment in zip(texts, sentiments):

            score = self._extract_score(sentiment)

            # Debug safeguard (remove later if stable)
            # print("DEBUG:", sentiment, "->", score)

            if score < 0:
                negative_count += 1
                text_lower = text.lower()

                # Performance Issues
                if any(word in text_lower for word in
                       ["slow", "delay", "lag", "sluggish", "loading"]):
                    root_causes["Performance Issues"] += 1

                # App Crashes
                elif any(word in text_lower for word in
                         ["crash", "freeze", "hang", "stuck"]):
                    root_causes["App Crashes"] += 1

                # Login / Authentication Problems
                elif any(word in text_lower for word in
                         ["login", "otp", "authentication", "verify", "password"]):
                    root_causes["Login Problems"] += 1

                # Update Issues
                elif "update" in text_lower:
                    root_causes["App Update Issues"] += 1

                # Payment / Transaction Issues
                elif any(word in text_lower for word in
                         ["payment", "transfer", "transaction", "qr", "bill"]):
                    root_causes["Payment Failures"] += 1

                # Accessibility / UX Issues
                elif any(word in text_lower for word in
                         ["accessibility", "dark mode", "developer option", "usability"]):
                    root_causes["Accessibility / UX Issues"] += 1

                else:
                    root_causes["Other Complaints"] += 1

        # --------------------------------------------------
        # EXECUTIVE OUTPUT
        # --------------------------------------------------
        if verbose:
            print("\n🔍 ROOT CAUSE BREAKDOWN:")

            if negative_count == 0:
                print("⚠ No negative reviews detected.")
                print("   → Sentiment polarity likely converted upstream.\n")
            else:
                print(f"Total Negative Reviews: {negative_count}\n")

                for cause, count in root_causes.most_common():
                    percentage = (count / negative_count) * 100
                    print(f"{cause}: {count} ({percentage:.1f}%)")

                print("\n📊 Root Cause Categories Identified:",
                      len(root_causes))
                print("-" * 45)

        return root_causes