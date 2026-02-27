from models.sentiment_model import SentimentModel
from models.embedding_model import EmbeddingModel
from models.topic_model import TopicModel

class TextProcessor:
    def __init__(self):
        self.sentiment_model = SentimentModel()
        self.embedding_model = EmbeddingModel()
        self.topic_model = TopicModel(n_clusters=2)

    def process(self, texts):
        sentiments = self.sentiment_model.predict_batch(texts)
        embeddings = self.embedding_model.encode(texts)
        topics = self.topic_model.fit_predict(embeddings)

        results = []

        for i, (text, sentiment) in enumerate(zip(texts, sentiments)):
            results.append({
                "text": text,
                "sentiment": sentiment["label"],
                "confidence": sentiment["score"],
                "topic_cluster": int(topics[i])
            })

        return results