import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (ULTIMATE VERSION) LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (ULTIMATE VERSION)\n")

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
            value = str(value).replace(",", "").replace("%", "").strip()
            return float(value)
        except:
            return None

    # -------------------------------
    def extract_from_df(self, df, sheet_name):

        results = {}

        if df.empty:
            return results

        sheet_lower = sheet_name.lower()

        # Skip irrelevant sheets
        if any(x in sheet_lower for x in ["change", "equity", "cash"]):
            self.log(f"{sheet_name}: Skipped")
            return results

        full_text = " ".join(df.astype(str).values.flatten()).lower()

        # Detect year
        year_match = re.search(r"(20\d{2})", full_text)
        if not year_match:
            self.warn(f"{sheet_name}: No year found")
            return results

        year = int(year_match.group(1))
        self.log(f"{sheet_name}: Year → {year}")

        # 🔥 ULTRA ROBUST PATTERNS
        patterns = {
            "revenue": r"(total operating income|total income|interest income|net interest income|operating income)[^0-9]{0,60}([\d,\.]+)",

            "net_profit": r"(net profit|net income|profit for the year|profit attributable to owners)[^0-9]{0,60}([\d,\.]+)",

            "operating_income": r"(operating income|operating profit|profit before tax|profit before income tax)[^0-9]{0,60}([\d,\.]+)",

            "total_assets": r"(total assets)[^0-9]{0,60}([\d,\.]+)",

            "roe": r"(return on equity|roe)[^0-9]{0,60}([\d\.]+)"
        }

        for metric, pattern in patterns.items():

            # Only extract revenue from income-type sheets
            if metric == "revenue" and not any(x in sheet_lower for x in ["income", "pl", "comprehensive"]):
                continue

            match = re.search(pattern, full_text)

            if match:
                value = self.clean_value(match.group(2))

                # Remove noise
                if value is not None and value > 1000:
                    results.setdefault(year, {})
                    results[year][metric] = value
                    self.log(f"{sheet_name}: {metric} → {value}")

        return results

    # -------------------------------
    def process_excel(self, file_path):
        all_results = {}

        self.log(f"\n📄 Processing: {file_path}")

        try:
            xls = pd.ExcelFile(file_path)

            for sheet in xls.sheet_names:

                df = xls.parse(sheet)

                extracted = self.extract_from_df(df, sheet)

                for year, metrics in extracted.items():
                    all_results.setdefault(year, {})
                    all_results[year].update(metrics)

        except Exception as e:
            self.warn(f"File error: {e}")

        self.log(f"📊 Extracted → {all_results}")
        return all_results

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