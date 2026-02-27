from models.sentiment_model import SentimentModel

class TextProcessor:
    def __init__(self):
        self.sentiment_model = SentimentModel()

    def process(self, texts):
        sentiments = self.sentiment_model.predict_batch(texts)

        results = []
        for text, sentiment in zip(texts, sentiments):
            results.append({
                "text": text,
                "sentiment": sentiment["label"],
                "confidence": sentiment["score"]
            })

        return results