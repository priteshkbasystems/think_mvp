from collections import Counter

class RootCauseAnalyzer:

    ROOT_CAUSE_MAP = {
        "Login Issue": ["login", "otp", "password", "verify", "cannot access"],
        "App Crash": ["crash", "freeze", "hang", "error", "bug"],
        "Update Problem": ["update", "new version", "after update"],
        "Payment Issue": ["transfer", "payment", "failed transaction"],
        "Performance Issue": ["slow", "lag", "loading", "delay"],
        "UI/UX Problem": ["confusing", "difficult", "complicated", "bad design"]
    }

    def analyze(self, texts, sentiments):
        """
        Only analyze negative reviews for root cause.
        """
        root_counter = Counter()

        for text, sentiment in zip(texts, sentiments):
            if sentiment < 0:  # Only negative sentiment
                text_lower = text.lower()
                for category, keywords in self.ROOT_CAUSE_MAP.items():
                    if any(keyword in text_lower for keyword in keywords):
                        root_counter[category] += 1

        return root_counter