import json
import os
import re
import sqlite3

import pandas as pd

from scripts.db_cache import init_db

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


class FinancialExtractor:

    def __init__(self):

        print("Loading Financial Metrics Extractor")

    def _extract_year(self, path):
        match = re.search(r"(20\\d{2})", os.path.basename(path))
        if match:
            return int(match.group(1))
        return None

    def _iter_financial_excels(self):
        if not os.path.exists(BASE_CORP_PATH):
            return

        for bank_folder in sorted(os.listdir(BASE_CORP_PATH)):
            bank_path = os.path.join(BASE_CORP_PATH, bank_folder)
            if not os.path.isdir(bank_path):
                continue

            fin_dir = os.path.join(bank_path, "financial_report")
            if not os.path.isdir(fin_dir):
                continue

            bank_name = bank_folder.replace("_", " ")

            for file in sorted(os.listdir(fin_dir)):
                if not (file.endswith(".xlsx") or file.endswith(".xls")):
                    continue
                yield bank_name, os.path.join(fin_dir, file)

    def run(self):

        init_db()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        inserted = 0

        for bank_name, file_path in self._iter_financial_excels():
            year = self._extract_year(file_path)
            if year is None:
                continue

            try:
                sheets = pd.read_excel(file_path, sheet_name=None)
            except Exception:
                continue

            for sheet_name, df in sheets.items():
                payload = {
                    "orient": "split",
                    "columns": [str(c) for c in df.columns.tolist()],
                    "index": [int(i) if isinstance(i, (int, float)) and float(i).is_integer() else str(i) for i in df.index.tolist()],
                    "data": df.astype(object).where(pd.notnull(df), None).values.tolist(),
                }

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO financial_statement_sheets
                    (bank_name, year, file_path, sheet_name, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (bank_name, year, file_path, str(sheet_name), json.dumps(payload, ensure_ascii=False)),
                )
                inserted += 1

        conn.commit()
        conn.close()

        print(f"Financial statements ingested (sheets): {inserted}")