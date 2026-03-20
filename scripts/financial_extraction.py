import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (UNIVERSAL ENGINE) LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (UNIVERSAL)\n")

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

    def clean_value(self, value):
        try:
            val = float(str(value).replace(",", "").replace("%", "").strip())

            # 🔥 VALID RANGE (BANK SCALE)
            if val < 1000:
                return None
            if val > 1e13:
                return None

            return val
        except:
            return None

    # -------------------------------
    # 🔥 CORE SMART EXTRACTION ENGINE
    # -------------------------------
    def find_metric_value(self, df, keywords, sheet_name):

        values_found = []

        matrix = df.astype(str).values
        rows, cols = matrix.shape

        for i in range(rows):
            for j in range(cols):

                cell = str(matrix[i][j]).lower()

                if any(k in cell for k in keywords):

                    # 🔥 SEARCH WINDOW (VERY IMPORTANT)
                    for x in range(max(0, i-2), min(rows, i+3)):
                        for y in range(max(0, j-2), min(cols, j+3)):

                            val = self.clean_value(matrix[x][y])

                            if val:
                                values_found.append(val)

        if not values_found:
            return None

        # 🔥 SMART SELECTION
        values_found = sorted(values_found)

        # Avoid extreme values
        filtered = [v for v in values_found if v < 1e12]

        if not filtered:
            return max(values_found)

        return filtered[-1]  # pick best realistic

    # -------------------------------
    def extract_from_df(self, df, sheet_name):

        if df.empty:
            return {}

        sheet_lower = sheet_name.lower()

        if any(x in sheet_lower for x in ["change", "equity", "cash", "cf"]):
            return {}

        text = " ".join(df.astype(str).values.flatten()).lower()

        year_match = re.search(r"(20\d{2})", text)
        if not year_match:
            return {}

        year = int(year_match.group(1))
        self.log(f"{sheet_name}: Year → {year}")

        results = {}

        # -------------------------------
        # 🔥 METRIC DEFINITIONS
        # -------------------------------
        metrics = {
            "total_assets": ["total assets"],
            "revenue": ["total operating income", "total income", "net interest income"],
            "net_profit": ["net profit", "profit for the year", "profit attributable"],
            "operating_income": ["profit before tax", "profit before income tax"]
        }

        for metric, keywords in metrics.items():

            # revenue only from income sheets
            if metric == "revenue" and not any(x in sheet_lower for x in ["income", "pl", "comprehensive"]):
                continue

            value = self.find_metric_value(df, keywords, sheet_name)

            if value:
                results[metric] = value
                self.log(f"{sheet_name}: {metric} → {value}")

        return {year: results} if results else {}

    # -------------------------------
    def process_excel(self, file_path):

        self.log(f"\n📄 Processing: {file_path}")

        try:
            all_results = {}

            xls = pd.ExcelFile(file_path)

            for sheet in xls.sheet_names:

                df = xls.parse(sheet)

                extracted = self.extract_from_df(df, sheet)

                for year, metrics in extracted.items():
                    all_results.setdefault(year, {})
                    all_results[year].update(metrics)

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

            files = os.listdir(path)

            excel_files = [
                f for f in files
                if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
            ]

            print(f"📄 Files: {excel_files}")

            for file in excel_files:

                file_path = os.path.join(path, file)

                results = self.process_excel(file_path)

                for year, metrics in results.items():

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