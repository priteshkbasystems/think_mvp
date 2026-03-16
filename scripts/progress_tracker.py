import sqlite3

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class ProgressTracker:

    def __init__(self):

        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

    def get_progress(self, step, bank, year):

        self.cursor.execute("""
        SELECT last_processed_index
        FROM step_progress
        WHERE step_name=? AND bank_name=? AND year=?
        """, (step, bank, year))

        row = self.cursor.fetchone()

        if row:
            return row[0]

        return 0

    def save_progress(self, step, bank, year, index):

        self.cursor.execute("""
        INSERT OR REPLACE INTO step_progress
        (step_name, bank_name, year, last_processed_index)
        VALUES (?, ?, ?, ?)
        """, (step, bank, year, index))

        self.conn.commit()