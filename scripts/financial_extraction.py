import os
import re
import sqlite3
import pandas as pd
import sys
sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")
DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (Excel - DEBUG MODE)\n")

        self.metric_keywords = {
            "revenue": ["revenue", "total revenue", "income"],
            "net_profit": ["net profit", "net income", "profit after tax"],
            "operating_income": ["operating income", "operating profit"],
            "total_assets": ["total assets"],
            "roe": ["roe", "return on equity"]
        }

    # -------------------------------
    def log(self, msg):
        print(f"[LOG] {msg}")

    def warn(self, msg):
        print(f"[WARNING] {msg}")

    # -------------------------------
    def normalize_name(self, name):
        return name.lower().replace(" ", "").replace("_", "")

    # -------------------------------
    def find_bank_folder(self, bank_name):

        if not os.path.exists(BASE_PATH):
            self.warn("Base path not found!")
            return None

        folders = os.listdir(BASE_PATH)

        for folder in folders:
            if self.normalize_name(folder) == self.normalize_name(bank_name):
                return os.path.join(BASE_PATH, folder)

        return None

    # -------------------------------
    def clean_value(self, value):
        if pd.isna(value):
            return None
        try:
            value = str(value).replace(",", "").replace("%", "").strip()
            return float(value)
        except:
            return None

    # -------------------------------
    def extract_years(self, columns):
        years = []
        for col in columns:
            match = re.search(r"(20\d{2})", str(col))
            if match:
                years.append((col, int(match.group(1))))
        return years

    # -------------------------------
    def match_metric(self, row_name):
        row_name = str(row_name).lower()

        for metric, keywords in self.metric_keywords.items():
            for keyword in keywords:
                if keyword in row_name:
                    return metric
        return None

    # -------------------------------
    def extract_from_df(self, df, sheet_name):
        results = {}

        if df.empty:
            self.warn(f"{sheet_name}: Empty sheet")
            return results

        df = df.dropna(how="all")

        first_col = df.columns[0]
        year_cols = self.extract_years(df.columns)

        if not year_cols:
            self.warn(f"{sheet_name}: No year columns detected")
            return results

        self.log(f"{sheet_name}: Years → {[y for _, y in year_cols]}")

        for _, row in df.iterrows():

            metric = self.match_metric(row[first_col])

            if not metric:
                continue

            for col, year in year_cols:

                value = self.clean_value(row[col])

                if value is None:
                    continue

                if year not in results:
                    results[year] = {}

                results[year][metric] = value

                self.log(f"{sheet_name}: {metric} | {year} → {value}")

        return results

    # -------------------------------
    def process_excel(self, file_path):
        all_results = {}

        self.log(f"\n📄 Processing file: {file_path}")

        try:
            xls = pd.ExcelFile(file_path)
            self.log(f"Sheets: {xls.sheet_names}")

            for sheet in xls.sheet_names:
                try:
                    df = xls.parse(sheet)
                    extracted = self.extract_from_df(df, sheet)

                    if not extracted:
                        self.warn(f"{sheet}: No data extracted")

                    for year, metrics in extracted.items():
                        if year not in all_results:
                            all_results[year] = {}

                        all_results[year].update(metrics)

                except Exception as e:
                    self.warn(f"{sheet} error: {e}")

        except Exception as e:
            self.warn(f"File error: {file_path} → {e}")

        self.log(f"📊 Extracted: {all_results}\n")

        return all_results

    # -------------------------------
    def run(self):

        print("\n🚀 STARTING FINANCIAL EXTRACTION DEBUG\n")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT bank_name FROM banks")
        banks = cursor.fetchall()

        print(f"\n📊 Banks from DB: {banks}\n")

        if not banks:
            print("❌ No banks found → EXIT")
            return

        for (bank,) in banks:

            print(f"\n🏦 Processing Bank: {bank}")

            bank_folder = self.find_bank_folder(bank)

            if not bank_folder:
                self.warn(f"Folder not found for bank: {bank}")
                continue

            base_path = os.path.join(bank_folder, "financial_report")

            print(f"📂 Financial path: {base_path}")

            if not os.path.exists(base_path):
                self.warn("financial_report folder missing")
                continue

            files = os.listdir(base_path)
            print(f"📄 Files: {files}")

            excel_files = [f for f in files if f.endswith((".xlsx", ".xls"))]

            if not excel_files:
                self.warn("No Excel files found")
                continue

            for file in excel_files:

                file_path = os.path.join(base_path, file)

                results = self.process_excel(file_path)

                if not results:
                    self.warn("No data extracted from file")
                    continue

                for year, metrics in results.items():

                    print(f"💾 Saving: {bank} | {year} | {metrics}")

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

        print("\n✅ FINANCIAL EXTRACTION COMPLETED\n")


# -------------------------------
def main():
    extractor = FinancialExtractor()
    extractor.run()