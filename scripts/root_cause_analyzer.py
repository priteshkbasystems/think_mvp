from collections import Counter


class RootCauseAnalyzer:
    """
    Root Cause Analyzer for negative customer reviews.
    Works with:
        1. Numeric sentiment scores (e.g., -0.75, 0.21)
        2. HuggingFace-style dict outputs:
           {"label": "NEGATIVE", "score": 0.92}
    """

    def __init__(self):
        pass

    def _extract_score(self, sentiment):
        """
        Normalize sentiment into a numeric score.
        Returns a float.
        """

        # Case 1: HuggingFace-style dictionary
        if isinstance(sentiment, dict):
            label = sentiment.get("label", "").upper()
            score = float(sentiment.get("score", 0))

            # Convert NEGATIVE label to negative numeric score
            if label == "NEGATIVE":
                return -score
            elif label == "POSITIVE":
                return score
            else:
                return 0.0

        # Case 2: Already numeric
        try:
            return float(sentiment)
        except Exception:
            return 0.0

    def analyze(self, texts, sentiments):
        """
        Analyze root causes for negative reviews only.
        Returns Counter object.
        """

        root_causes = Counter()

        for text, sentiment in zip(texts, sentiments):

            score = self._extract_score(sentiment)

            # Only analyze negative sentiment
            if score < 0:

                text_lower = text.lower()

                # Performance Issues
                if any(word in text_lower for word in [
                    "slow", "delay", "lag", "sluggish", "loading"
                ]):
                    root_causes["Performance Issues"] += 1

                # App Crashes
                elif any(word in text_lower for word in [
                    "crash", "freeze", "hang", "stuck"
                ]):
                    root_causes["App Crashes"] += 1

                # Login / Authentication Problems
                elif any(word in text_lower for word in [
                    "login", "otp", "authentication", "verify", "password"
                ]):
                    root_causes["Login Problems"] += 1

                # Update Issues
                elif "update" in text_lower:
                    root_causes["App Update Issues"] += 1

                # Payment / Transaction Issues
                elif any(word in text_lower for word in [
                    "payment", "transfer", "transaction", "qr", "bill"
                ]):
                    root_causes["Payment Failures"] += 1

                # Accessibility / UX Issues
                elif any(word in text_lower for word in [
                    "accessibility", "dark mode", "developer option", "usability"
                ]):
                    root_causes["Accessibility / UX Issues"] += 1

                else:
                    root_causes["Other Complaints"] += 1

        return root_causes