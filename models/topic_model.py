from sklearn.cluster import KMeans
import numpy as np


class TopicModel:

    def __init__(self, n_clusters=5):

        self.n_clusters = n_clusters

        self.model = KMeans(
            n_clusters=self.n_clusters,
            random_state=42,
            n_init=10
        )


    def fit_predict(self, embeddings):

        # Ensure numpy array
        embeddings = np.asarray(embeddings)

        # Handle empty input
        if len(embeddings) == 0:
            return np.array([])

        # Prevent crash when samples < clusters
        if len(embeddings) < self.n_clusters:
            return np.zeros(len(embeddings), dtype=int)

        return self.model.fit_predict(embeddings)