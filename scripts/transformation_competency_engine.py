import sqlite3
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

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

        self.model = SentenceTransformer("all-MiniLM-L6-v2")

        self.comp_embeddings = self.model.encode(COMPETENCIES)

    def compute(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT file_path, year
        FROM pdf_cache
        """)

        rows = cursor.fetchall()

        results = {}

        for path, year in rows:

            bank = path.split("/")[-3]

            text = path

            emb = self.model.encode([text])[0]

            sims = cosine_similarity([emb], self.comp_embeddings)[0]

            for i, comp in enumerate(COMPETENCIES):

                score = float(sims[i])

                cursor.execute("""
                INSERT INTO transformation_competencies
                (bank_name, year, competency, score)
                VALUES (?, ?, ?, ?)
                """, (bank, year, comp, score))

                results.setdefault(bank, {})
                results[bank][comp] = score

        conn.commit()
        conn.close()

        return results