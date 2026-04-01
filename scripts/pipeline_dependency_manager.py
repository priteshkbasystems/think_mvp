import sqlite3

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


DEPENDENCIES = {

    "STEP 2 — SENTIMENT TREND": ["STEP 1 — DATA INDEXING"],

    "STEP 3 — TRANSFORMATION INTELLIGENCE": ["STEP 1 — DATA INDEXING"],

    "STEP 5 — NARRATIVE SCORES": ["STEP 4 — CORPORATE TOPIC SENTIMENT"],

    "STEP 6 — TOPIC ALIGNMENT": [
        "STEP 4 — CORPORATE TOPIC SENTIMENT",
        "STEP 7 — ASPECT SENTIMENT"
    ],

    "STEP 11 — SCENARIO SIMULATION": [
        "STEP 5 — NARRATIVE SCORES",
        "STEP 2 — SENTIMENT TREND"
    ]
}


class PipelineDependencyManager:

    def __init__(self):

        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

    def dependency_changed(self, step):

        if step not in DEPENDENCIES:
            return False

        deps = DEPENDENCIES[step]

        for d in deps:

            self.cursor.execute("""
            SELECT status
            FROM pipeline_runs
            WHERE step_name=?
            """, (d,))

            row = self.cursor.fetchone()

            if not row or row[0] != "SUCCESS":
                return True

        return False