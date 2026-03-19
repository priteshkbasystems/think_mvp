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
        print("📊 Loading Financial Metrics Extractor")

    # -----------------------------------------
    # Extract year from file or path
    # -----------------------------------------
    def _extract_year(self, path):

        match = re.search(r"(20\d{2})", path)

        if match:
            return int(match.group(1))

        return None

    # -----------------------------------------
    # Iterate Excel files
    # -----------------------------------------
    def _iter_financial_excels(self):

        if not os.path.exists(BASE_CORP_PATH):
            print("❌ Base path not found")
            return

        for bank_folder in sorted(os.listdir(BASE_CORP_PATH)):

            bank_path = os.path.join(BASE_CORP_PATH, bank_folder)

            if not os.path.isdir(bank_path):
                continue

            fin_dir = os.path.join(bank_path, "financial_report")

            if not os.path.isdir(fin_dir):
                continue

            bank_name = bank_folder  # keep consistent (NO replace)

            for file in sorted(os.listdir(fin_dir)):

                if not (file.endswith(".xlsx") or file.endswith(".xls")):
                    continue

                yield bank_name, os.path.join(fin_dir, file)

    # -----------------------------------------
    # Create table if not exists
    # -----------------------------------------
    def _create_table(self, cursor):

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_statement_sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT,
            year INTEGER,
            file_path TEXT,
            sheet_name TEXT,
            payload_json TEXT,
            UNIQUE(bank_name, year, file_path, sheet_name)
        )
        """)

    # -----------------------------------------
    # Main run
    # -----------------------------------------
    def run(self):

        init_db()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        self._create_table(cursor)

        inserted = 0
        skipped = 0

        for bank_name, file_path in self._iter_financial_excels():

            year = self._extract_year(file_path)

            if year is None:
                print(f"⚠ Year not found: {file_path}")
                continue

            print(f"\n🏦 {bank_name} | 📄 {os.path.basename(file_path)} | 📅 {year}")

            try:
                sheets = pd.read_excel(file_path, sheet_name=None)
            except Exception as e:
                print("❌ Failed to read:", file_path, e)
                continue

            for sheet_name, df in sheets.items():

                if df.empty or df.shape[0] < 2:
                    skipped += 1
                    continue

                try:
                    payload = {
                        "columns": [str(c) for c in df.columns.tolist()],
                        "data": df.astype(object)
                                  .where(pd.notnull(df), None)
                                  .values.tolist()
                    }

                    cursor.execute("""
                    INSERT OR IGNORE INTO financial_statement_sheets
                    (bank_name, year, file_path, sheet_name, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """, (
                        bank_name,
                        year,
                        file_path,
                        str(sheet_name),
                        json.dumps(payload, ensure_ascii=False)
                    ))

                    inserted += 1

                except Exception as e:
                    print(f"⚠ Sheet error: {sheet_name}", e)

        conn.commit()
        conn.close()

        print("\n✅ Financial ingestion completed")
        print(f"✔ Inserted: {inserted}")
        print(f"⚠ Skipped: {skipped}")