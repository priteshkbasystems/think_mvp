import sqlite3
import numpy as np

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class TopicMappingEngine:

    def __init__(self):

        print("Loading Topic Mapping Engine...")

        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    # -----------------------------------------
    # Load customer topics
    # -----------------------------------------
    def load_topics(self, cursor):

        cursor.execute("""
        SELECT topic_id, keywords
        FROM complaint_topics
        """)

        rows = cursor.fetchall()

        topic_map = {}

        for topic_id, keywords in rows:

            if not keywords:
                continue

            words = keywords.split(",")

            topic_map[topic_id] = words

        return topic_map

    # -----------------------------------------
    # Encode topics
    # -----------------------------------------
    def encode_topics(self, topic_map):

        topic_embeddings = {}

        for topic_id, keywords in topic_map.items():

            text = " ".join(keywords)

            emb = self.model.encode([text])[0]

            topic_embeddings[topic_id] = emb

        return topic_embeddings

    # -----------------------------------------
    # Map reviews → topic_id
    # -----------------------------------------
    def map_reviews(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Load topics
        topic_map = self.load_topics(cursor)

        if not topic_map:
            print("❌ No topics found")
            return

        topic_embeddings = self.encode_topics(topic_map)

        topic_ids = list(topic_embeddings.keys())
        topic_matrix = np.array(list(topic_embeddings.values()))

        print(f"Loaded {len(topic_ids)} topics")

        # -----------------------------------------
        # Fetch unmapped reviews
        # -----------------------------------------
        cursor.execute("""
        SELECT id, review_text
        FROM review_sentiments
        WHERE topic_id IS NULL
        """)

        reviews = cursor.fetchall()

        if not reviews:
            print("✔ All reviews already mapped")
            return

        print(f"Mapping {len(reviews)} reviews...")

        # -----------------------------------------
        # Encode reviews
        # -----------------------------------------
        texts = [r[1] for r in reviews]

        review_embeddings = self.model.encode(texts, batch_size=128)

        # -----------------------------------------
        # Compute similarity
        # -----------------------------------------
        sim = cosine_similarity(review_embeddings, topic_matrix)

        # -----------------------------------------
        # Assign best topic
        # -----------------------------------------
        updates = []

        for i, row in enumerate(reviews):

            review_id = row[0]

            best_idx = np.argmax(sim[i])
            best_topic = topic_ids[best_idx]

            updates.append((best_topic, review_id))

        # -----------------------------------------
        # Bulk update
        # -----------------------------------------
        cursor.executemany("""
        UPDATE review_sentiments
        SET topic_id=?
        WHERE id=?
        """, updates)

        conn.commit()
        conn.close()

        print("✅ Topic mapping completed")

    # -----------------------------------------
    # Run
    # -----------------------------------------
    def run(self):

        self.map_reviews()