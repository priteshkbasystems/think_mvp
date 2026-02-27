from collections import Counter


class RootCauseAnalyzer:
    """
    Root Cause Analyzer for negative customer reviews.
    Supports:
        1. Numeric sentiment scores (e.g., -0.75, 0.21)
        2. HuggingFace-style dict outputs:
           {"label": "NEGATIVE", "score": 0.92}
    """

    def __init__(self):
        pass

    # --------------------------------------------------
    # SENTIMENT NORMALIZATION
    # --------------------------------------------------
    def _extract_score(self, sentiment):
        """
        Normalize sentiment into numeric score.
        Always returns float.
        """

        # Case 1: Numeric (int, float, numpy.float)
        if isinstance(sentiment, (int, float)):
            return float(sentiment)

        # Case 2: HuggingFace-style dictionary
        if isinstance(sentiment, dict):

            # Standard HF format
            if "label" in sentiment:
                label = sentiment.get("label", "").upper()
                score = float(sentiment.get("score", 0))

                if label == "NEGATIVE":
                    return -score
                elif label == "POSITIVE":
                    return score
                else:
                    return 0.0

            # Custom probability format
            if "negative" in sentiment:
                return -float(sentiment.get("negative", 0))

        # Fallback
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

            # Only analyze negative sentiment
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
        # PRINT RESULTS
        # --------------------------------------------------
        if verbose:
            print("\n🔍 ROOT CAUSE BREAKDOWN:")

            if negative_count == 0:
                print("⚠ No negative reviews detected.")
                print("   (Check sentiment format if unexpected)\n")
            else:
                print(f"Total Negative Reviews: {negative_count}\n")

                for cause, count in root_causes.most_common():
                    percentage = (count / negative_count) * 100
                    print(f"{cause}: {count} ({percentage:.1f}%)")

                print("\n📊 Root Cause Categories Identified:",
                      len(root_causes))
                print("-" * 40)

        return root_causes