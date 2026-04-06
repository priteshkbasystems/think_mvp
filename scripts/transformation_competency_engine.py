import sqlite3
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from models.embedding_model import EmbeddingModel

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"

COMPETENCIES = [
    "digital transformation capability",
    "customer experience transformation",
    "operational excellence",
    "automation capability",
    "leadership transformation"
]


class TransformationCompetencyEngine:

    def __init__(self):

        print("Loading Transformation Competency Engine")

        self.model = EmbeddingModel()

        self.comp_embeddings = self.model.encode(COMPETENCIES)

    def compute(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT bank_id, bank_name, year, sentence_text
        FROM corporate_sentence_sentiment
        """)

        rows = cursor.fetchall()

        results = {}
        grouped = {}

        for bank_id, bank, year, sentence_text in rows:
            key = (bank_id, bank, year)
            grouped.setdefault(key, [])
            if sentence_text:
                grouped[key].append(sentence_text)

        for (bank_id, bank, year), sentences in grouped.items():

            if not sentences:
                continue

            text = " ".join(sentences[:500])

            emb = self.model.encode([text])[0]

            sims = cosine_similarity([emb], self.comp_embeddings)[0]

            for i, comp in enumerate(COMPETENCIES):

                score = float(sims[i])

                cursor.execute("""
                INSERT INTO transformation_competencies
                (bank_id, bank_name, year, competency, score)
                VALUES (?, ?, ?, ?, ?)
                """, (bank_id, bank, year, comp, score))

                results.setdefault(bank, {})
                results[bank][comp] = score

        conn.commit()
        conn.close()

        return results