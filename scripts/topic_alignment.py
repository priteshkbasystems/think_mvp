import numpy as np
from sentence_transformers import SentenceTransformer


class TopicAlignmentEngine:

    def __init__(self):

        print("Loading Topic Alignment Engine...")

        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def align_topics(self, corporate_topics, customer_topics):

        if not corporate_topics or not customer_topics:
            return []

        corp_emb = self.model.encode(corporate_topics)
        cust_emb = self.model.encode(customer_topics)

        alignments = []

        for i, c_topic in enumerate(corporate_topics):

            best_topic = None
            best_score = -1

            for j, u_topic in enumerate(customer_topics):

                score = np.dot(corp_emb[i], cust_emb[j])

                if score > best_score:
                    best_score = score
                    best_topic = u_topic

            alignments.append({
                "corporate_topic": c_topic,
                "customer_topic": best_topic,
                "similarity": float(best_score)
            })

        return alignments