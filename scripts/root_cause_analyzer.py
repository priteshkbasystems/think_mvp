from collections import Counter
import numbers


class RootCauseAnalyzer:

    def _extract_score(self, sentiment):
        """
        Convert any sentiment format into signed numeric polarity.
        """

        original = sentiment  # keep for debug

        # 🔹 unwrap nested lists
        while isinstance(sentiment, list) and len(sentiment) > 0:
            sentiment = sentiment[0]

        # 🔹 torch tensor support
        try:
            import torch
            if isinstance(sentiment, torch.Tensor):
                sentiment = sentiment.item()
        except Exception:
            pass

        # 🔹 numeric types (int, float, numpy, etc.)
        if isinstance(sentiment, numbers.Number):
            return float(sentiment)

        # 🔹 dictionary formats
        if isinstance(sentiment, dict):

            if "label" in sentiment:
                label = str(sentiment.get("label", "")).upper()
                confidence = float(sentiment.get("score", 0))

                if label == "NEGATIVE":
                    return -confidence
                elif label == "POSITIVE":
                    return confidence
                else:
                    return 0.0

            if "negative" in sentiment:
                return -float(sentiment.get("negative", 0))

            if "positive" in sentiment:
                return float(sentiment.get("positive", 0))

        # 🔹 fallback
        try:
            return float(sentiment)
        except Exception:
            print("⚠ Could not convert sentiment:", original)
            return 0.0

    # --------------------------------------------------

    def analyze(self, texts, sentiments, verbose=True):

        print("\n🛠 DEBUG: Inspecting Sentiment Input ----------------")

        for i in range(min(5, len(sentiments))):
            raw = sentiments[i]
            extracted = self._extract_score(raw)
            print(f"[{i}] RAW TYPE: {type(raw)} | RAW VALUE: {raw}")
            print(f"     → EXTRACTED SCORE: {extracted}")

        print("----------------------------------------------------\n")

        root_causes = Counter()
        negative_count = 0
        positive_only_counter = 0

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

            else:
                positive_only_counter += 1

        # --------------------------------------------------

        if verbose:
            print("\n🔍 ROOT CAUSE BREAKDOWN:")

            print(f"Total Reviews: {len(sentiments)}")
            print(f"Negative Detected: {negative_count}")
            print(f"Non-Negative Detected: {positive_only_counter}\n")

            if negative_count == 0:
                print("🚨 CRITICAL: No negative reviews detected.")
                print("This means sentiments passed to RCA are NOT negative values.")
                print("Fix required in TextProcessor.process()")
                print("--------------------------------------------------\n")
            else:
                for cause, count in root_causes.most_common():
                    percentage = (count / negative_count) * 100
                    print(f"{cause}: {count} ({percentage:.1f}%)")

                print("\n📊 Root Cause Categories Identified:",
                      len(root_causes))
                print("-" * 45)

        return root_causes