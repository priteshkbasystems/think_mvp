import sqlite3

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class ProgressTracker:

    def __init__(self):

        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

    def get_progress(self, step, bank, year):
        self.cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank,))
        row = self.cursor.fetchone()
        if not row:
            return 0
        bank_id = row[0]

        self.cursor.execute("""
        SELECT last_processed_index
        FROM step_progress
        WHERE step_name=? AND bank_id=? AND year=?
        """, (step, bank_id, year))

        row = self.cursor.fetchone()

        if row:
            return row[0]

        return 0

    def save_progress(self, step, bank, year, index):
        self.cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank,))
        row = self.cursor.fetchone()
        if not row:
            return
        bank_id = row[0]

        self.cursor.execute("""
        INSERT OR REPLACE INTO step_progress
        (step_name, bank_id, bank_name, year, last_processed_index)
        VALUES (?, ?, ?, ?, ?)
        """, (step, bank_id, bank, year, index))

        self.conn.commit()