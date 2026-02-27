from sklearn.cluster import KMeans
import numpy as np

class TopicModel:
    def __init__(self, n_clusters=2):
        self.n_clusters = n_clusters
        self.model = KMeans(n_clusters=self.n_clusters, random_state=42)

    def fit_predict(self, embeddings):
        embeddings_np = embeddings.cpu().numpy()
        return self.model.fit_predict(embeddings_np)