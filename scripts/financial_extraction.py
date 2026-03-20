import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (UNIVERSAL ENGINE v2) LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (STRUCTURE-AWARE)\n")

    def log(self, msg):
        print(f"[LOG] {msg}")

    def warn(self, msg):
        print(f"[WARNING] {msg}")

    def normalize_name(self, name):
        return name.lower().replace(" ", "").replace("_", "")

    def find_bank_folder(self, bank_name):
        for folder in os.listdir(BASE_PATH):
            if self.normalize_name(folder) == self.normalize_name(bank_name):
                return os.path.join(BASE_PATH, folder)
        return None

    # -------------------------------
    # CLEAN VALUE
    # -------------------------------
    def clean_value(self, value):
        try:
            value = str(value).replace(",", "").replace("%", "").strip()

            if value == "" or value.lower() in ["nan", "none"]:
                return None

            val = float(value)

            if 1900 < val < 2100:
                return None

            if val < 1000:
                return None

            if val > 1e13:
                return None

            return val

        except:
            return None

    # -------------------------------
    # ROE
    # -------------------------------
    def compute_roe(self, metrics):
        try:
            np = metrics.get("net_profit")
            eq = metrics.get("equity")

            if np and eq and eq != 0:
                return round((np / eq) * 100, 2)
        except:
            pass
        return None

    # -------------------------------
    # 🔥 CORE STRUCTURE ENGINE
    # -------------------------------
    def extract_from_df(self, df, sheet_name):

        if df.empty:
            return {}

        df = df.fillna("")
        df.columns = range(len(df.columns))  # normalize

        sheet_lower = sheet_name.lower()

        # skip only cashflow
        if any(x in sheet_lower for x in ["cash", "cf"]):
            return {}

        # -------------------------------
        # DETECT YEAR COLUMNS
        # -------------------------------
        year_cols = {}

        for col in df.columns:
            for val in df[col].astype(str):
                match = re.search(r"(20\d{2})", val)
                if match:
                    year_cols[col] = int(match.group(1))

        if not year_cols:
            return {}

        results = {}

        keyword_map = {
            "revenue": [
                "total operating income",
                "total income",
                "interest income"
            ],
            "net_profit": [
                "net profit",
                "profit for the year",
                "profit attributable"
            ],
            "operating_income": [
                "profit before tax"
            ],
            "total_assets": [
                "total assets"
            ],
            "equity": [
                "total equity",
                "shareholders",
                "equity attributable"
            ]
        }

        # -------------------------------
        # ROW → COLUMN MATCH
        # -------------------------------
        for i in range(len(df)):

            row = df.iloc[i]
            row_text = " ".join(row.astype(str)).lower()

            for metric, keywords in keyword_map.items():

                if any(k in row_text for k in keywords):

                    for col_idx, year in year_cols.items():

                        val = row[col_idx]
                        clean_val = self.clean_value(val)

                        if clean_val:
                            results.setdefault(year, {})
                            results[year][metric] = clean_val

                            self.log(f"{sheet_name}: {metric} → {clean_val} ({year})")

        return results

    # -------------------------------
    def process_excel(self, file_path):

        self.log(f"\n📄 Processing: {file_path}")

        try:
            xls = pd.ExcelFile(file_path)
            all_results = {}

            for sheet in xls.sheet_names:
                df = xls.parse(sheet)

                extracted = self.extract_from_df(df, sheet)

                for year, metrics in extracted.items():
                    all_results.setdefault(year, {})

                    for k, v in metrics.items():
                        if v:
                            all_results[year][k] = v

            return all_results

        except Exception as e:
            self.warn(f"File error: {e}")
            return {}

    # -------------------------------
    def run(self):

        print("\n🚀 STARTING EXTRACTION\n")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT bank_name FROM banks")
        banks = cursor.fetchall()

        print(f"📊 Banks: {banks}")

        for (bank,) in banks:

            print(f"\n🏦 {bank}")

            bank_folder = self.find_bank_folder(bank)

            if not bank_folder:
                self.warn("Bank folder not found")
                continue

            path = os.path.join(bank_folder, "financial_report")

            if not os.path.exists(path):
                self.warn("financial_report missing")
                continue

            excel_files = [
                f for f in os.listdir(path)
                if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
            ]

            print(f"📄 Files: {excel_files}")

            for file in excel_files:

                file_path = os.path.join(path, file)

                results = self.process_excel(file_path)

                for year, metrics in results.items():

                    # 🔥 ROE
                    metrics["roe"] = self.compute_roe(metrics)

                    print(f"💾 Saving → {bank} | {year} | {metrics}")

                    cursor.execute("""
                    INSERT OR REPLACE INTO financial_metrics
                    (bank_name, year, revenue, net_profit, operating_income, total_assets, roe)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        bank,
                        year,
                        metrics.get("revenue"),
                        metrics.get("net_profit"),
                        metrics.get("operating_income"),
                        metrics.get("total_assets"),
                        metrics.get("roe")
                    ))

        conn.commit()
        conn.close()

        print("\n✅ DONE\n")


def main():
    FinancialExtractor().run()