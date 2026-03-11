from bertopic import BERTopic
from sentence_transformers import SentenceTransformer


class ComplaintTopicDiscovery:

    def __init__(self):

        print("Loading topic discovery model...")

        self.embedding_model = SentenceTransformer(
            "all-MiniLM-L6-v2"
        )

        self.topic_model = BERTopic(
            embedding_model=self.embedding_model,
            min_topic_size=5
        )

    def discover_topics(self, texts):

        if not texts:
            return {}

        topics, probs = self.topic_model.fit_transform(texts)

        topic_info = self.topic_model.get_topic_info()

        results = {}

        for topic_id in topic_info.Topic:

            if topic_id == -1:
                continue

            words = self.topic_model.get_topic(topic_id)

            keywords = [w[0] for w in words[:5]]

            results[topic_id] = keywords

        return results