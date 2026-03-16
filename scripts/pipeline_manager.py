import sqlite3
from datetime import datetime

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class PipelineManager:

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

    def should_run(self, step):

        self.cursor.execute("""
        SELECT status
        FROM pipeline_runs
        WHERE step_name=?
        """, (step,))

        row = self.cursor.fetchone()

        if row and row[0] == "success":
            return False

        return True

    def mark_success(self, step):

        self.cursor.execute("""
        INSERT OR REPLACE INTO pipeline_runs
        (step_name, last_run, status)
        VALUES (?, ?, ?)
        """, (step, datetime.now(), "success"))

        self.conn.commit()

    def mark_failed(self, step):

        self.cursor.execute("""
        INSERT OR REPLACE INTO pipeline_runs
        (step_name, last_run, status)
        VALUES (?, ?, ?)
        """, (step, datetime.now(), "failed"))

        self.conn.commit()