import sqlite3
import os
import re

from models.sentiment_model import SentimentModel
from scripts.transformation_correlation import extract_text_from_pdf
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
        SELECT file_path, year
        FROM pdf_cache
        """)

        rows = cursor.fetchall()

        # run documents in parallel
        results = self.executor.run(self.process_document, rows)

        # remove failed jobs
        results = [r for r in results if r]

        cursor.executemany("""
        INSERT OR REPLACE INTO corporate_sentiment
        (bank_name, year, sentiment)
        VALUES (?, ?, ?)
        """, results)

        conn.commit()
        conn.close()

        print("Corporate sentiment analysis complete.")