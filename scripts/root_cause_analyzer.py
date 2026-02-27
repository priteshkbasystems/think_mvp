from collections import Counter


class RootCauseAnalyzer:
    """
    Enterprise Root Cause Analyzer for customer reviews.

    Supports:
        - Numeric sentiment scores (-0.75, 0.32)
        - HuggingFace list output [[{'label':..., 'score':...}]]
        - HuggingFace dict output {'label':..., 'score':...}
        - Custom probability format {'negative': 0.91}
    """

    # --------------------------------------------------
    # SENTIMENT NORMALIZATION (BULLETPROOF)
    # --------------------------------------------------
    def _extract_score(self, sentiment):
        """
        Convert any sentiment format into numeric polarity.
        Always returns float.
        """

        # 1️⃣ Handle HuggingFace nested list format
        if isinstance(sentiment, list) and len(sentiment) > 0:
            sentiment = sentiment[0]

        # 2️⃣ Numeric types (int, float, numpy.float)
        if isinstance(sentiment, (int, float)):
            return float(sentiment)

        # 3️⃣ Dictionary formats
        if isinstance(sentiment, dict):

            # HuggingFace standard format
            if "label" in sentiment:
                label = str(sentiment.get("label", "")).upper()
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

        # 4️⃣ Fallback safe conversion
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
        # EXECUTIVE PRINT OUTPUT
        # --------------------------------------------------
        if verbose:
            print("\n🔍 ROOT CAUSE BREAKDOWN:")

            if negative_count == 0:
                print("⚠ No negative reviews detected.")
                print("   (Sentiment format mismatch or all reviews positive)\n")
            else:
                print(f"Total Negative Reviews: {negative_count}\n")

                for cause, count in root_causes.most_common():
                    percentage = (count / negative_count) * 100
                    print(f"{cause}: {count} ({percentage:.1f}%)")

                print("\n📊 Root Cause Categories Identified:",
                      len(root_causes))
                print("-" * 45)

        return root_causes