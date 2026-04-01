import sqlite3
import os
import re

from models.sentiment_model import SentimentModel
from scripts.corporate_pdf_utils import extract_text_from_pdf, is_allowed_corporate_pdf
from scripts.parallel_executor import ParallelExecutor


DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class CorporateSentimentModel:

    def __init__(self):

        print("Loading Corporate Sentiment Model")

        self.model = SentimentModel()

        # run documents in parallel
        self.executor = ParallelExecutor(workers=4)


    # ---------------------------------------
    # Clean and split document text
    # ---------------------------------------

    def preprocess_text(self, text):

        if not text:
            return []

        text = re.sub(r"\s+", " ", text)

        sentences = text.split(".")

        # limit size for performance
        sentences = sentences[:120]

        sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

        return sentences


    # ---------------------------------------
    # Sentiment scoring
    # ---------------------------------------

    def analyze_document(self, text):

        sentences = self.preprocess_text(text)

        if not sentences:
            return 0.0

        preds = self.model.predict_batch(sentences)

        scores = []

        for p in preds:

            score = float(p["score"])

            if p["label"] == "NEGATIVE":
                score = -score

            scores.append(score)

        return sum(scores) / len(scores)


    # ---------------------------------------
    # Extract bank name safely
    # ---------------------------------------

    def get_bank_name(self, path):

        parts = os.path.normpath(path).split(os.sep)

        if len(parts) >= 3:
            return parts[-3]

        return "unknown_bank"


    # ---------------------------------------
    # Process single document (for parallel)
    # ---------------------------------------

    def process_document(self, row):

        path, year = row

        try:

            bank = self.get_bank_name(path)

            text = extract_text_from_pdf(path)

            score = self.analyze_document(text)

            return (bank, year, score)

        except Exception as e:

            print("Error processing:", path, e)

            return None


    # ---------------------------------------
    # Main execution
    # ---------------------------------------

    def run(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT bank_id, bank_name, year, AVG(signed_score) AS sentiment
        FROM corporate_sentence_sentiment
        GROUP BY bank_id, bank_name, year
        """)

        upsert_rows = [(r[0], r[1], r[2], r[3]) for r in cursor.fetchall() if r[0] is not None]

        cursor.executemany("""
        INSERT OR REPLACE INTO corporate_sentiment
        (bank_id, bank_name, year, sentiment)
        VALUES (?, ?, ?, ?)
        """, upsert_rows)

        conn.commit()
        conn.close()

        print("Corporate sentiment analysis complete.")