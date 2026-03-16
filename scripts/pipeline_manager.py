import sqlite3
from datetime import datetime

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class PipelineManager:

    def __init__(self):

        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

        # ensure table exists
        self.init_table()

    def init_table(self):

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            step_name TEXT PRIMARY KEY,
            status TEXT,
            last_run TIMESTAMP
        )
        """)

        self.conn.commit()

    # -------------------------------------
    # Check if step should run
    # -------------------------------------

    def should_run(self, step_name):

        self.cursor.execute("""
        SELECT status
        FROM pipeline_runs
        WHERE step_name=?
        """, (step_name,))

        row = self.cursor.fetchone()

        # step never executed before
        if not row:
            return True

        # rerun only if failed
        if row[0] != "SUCCESS":
            return True

        return False

    # -------------------------------------
    # Mark success
    # -------------------------------------

    def mark_success(self, step_name):

        self.cursor.execute("""
        INSERT OR REPLACE INTO pipeline_runs
        (step_name, status, last_run)
        VALUES (?, ?, ?)
        """, (step_name, "SUCCESS", datetime.utcnow()))

        self.conn.commit()

    # -------------------------------------
    # Mark failure
    # -------------------------------------

    def mark_failed(self, step_name):

        self.cursor.execute("""
        INSERT OR REPLACE INTO pipeline_runs
        (step_name, status, last_run)
        VALUES (?, ?, ?)
        """, (step_name, "FAILED", datetime.utcnow()))

        self.conn.commit()