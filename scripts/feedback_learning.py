import sqlite3

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class FeedbackLearning:

    def __init__(self):
        print("Loading Feedback Learning Module")

    def check_new_labels(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT COUNT(*)
        FROM human_labels
        WHERE human_label IS NOT NULL
        """)

        count = cursor.fetchone()[0]

        conn.close()

        return count