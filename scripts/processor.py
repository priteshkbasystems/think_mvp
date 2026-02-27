from models.sentiment_model import SentimentModel
from models.embedding_model import EmbeddingModel

class TextProcessor:
    def __init__(self):
        self.sentiment_model = SentimentModel()
        self.embedding_model = EmbeddingModel()

    def process(self, texts):
        sentiments = self.sentiment_model.predict_batch(texts)
        embeddings = self.embedding_model.encode(texts)

        results = []

        for i, (text, sentiment) in enumerate(zip(texts, sentiments)):
            results.append({
                "text": text,
                "sentiment": sentiment["label"],
                "confidence": sentiment["score"],
                "embedding_vector_length": len(embeddings[i])
            })

        return results