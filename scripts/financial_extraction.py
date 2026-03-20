import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (BANGKOK + UNIVERSAL) LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (FINAL)\n")

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

            if val < 1000 or val > 1e13:
                return None

            return val
        except:
            return None

    # =========================================================
    # 🔥 BANGKOK SPECIAL EXTRACTION (CUSTOM LOGIC)
    # =========================================================
    def extract_bangkok_special(self, file_path):

        self.log("🔥 Bangkok Special Extraction")

        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None)

            df = pd.concat(all_sheets.values(), ignore_index=True)

            text = " ".join(df.astype(str).values.flatten()).lower()

            year_match = re.search(r"(20\d{2})", text)
            if not year_match:
                return {}

            year = int(year_match.group(1))
            results = {year: {}}

            # -------------------------------
            # REGEX EXTRACTION
            # -------------------------------
            patterns = {
                "revenue": r"(net interest income|total operating income)[^0-9]{0,50}([\d,]+)",
                "net_profit": r"(profit attributable|net profit)[^0-9]{0,50}([\d,]+)",
                "total_assets": r"(total assets)[^0-9]{0,50}([\d,]+)"
            }

            for metric, pattern in patterns.items():
                match = re.search(pattern, text)
                if match:
                    value = self.clean_value(match.group(2))
                    if value:
                        results[year][metric] = value
                        self.log(f"Bangkok regex {metric} → {value}")

            # -------------------------------
            # ROW SCAN (fallback)
            # -------------------------------
            keyword_map = {
                "net_profit": ["profit attributable", "net profit"],
                "total_assets": ["total assets"]
            }

            for i, row in df.iterrows():
                row_text = " ".join([str(x).lower() for x in row.values])

                for metric, keywords in keyword_map.items():

                    if metric in results[year]:
                        continue

                    if any(k in row_text for k in keywords):

                        for val in row.values:
                            value = self.clean_value(val)
                            if value:
                                results[year][metric] = value
                                self.log(f"Bangkok row {metric} → {value}")
                                break

            # -------------------------------
            # GLOBAL MAX (last fallback)
            # -------------------------------
            if "total_assets" not in results[year]:

                numbers = []

                for val in df.values.flatten():
                    value = self.clean_value(val)
                    if value:
                        numbers.append(value)

                if numbers:
                    max_val = max(numbers)
                    results[year]["total_assets"] = max_val
                    self.log(f"Bangkok fallback total_assets → {max_val}")

            return results

        except Exception as e:
            self.warn(f"Bangkok error: {e}")
            return {}

    # =========================================================
    # 🔥 STANDARD EXTRACTION (OTHER BANKS)
    # =========================================================
    def find_metric_value(self, df, keywords):

        matrix = df.astype(str).values
        rows, cols = matrix.shape

        values = []

        for i in range(rows):
            for j in range(cols):

                cell = str(matrix[i][j]).lower()

                if any(k in cell for k in keywords):

                    for x in range(max(0, i-2), min(rows, i+3)):
                        for y in range(max(0, j-2), min(cols, j+3)):

                            val = self.clean_value(matrix[x][y])

                            if val:
                                values.append(val)

        if not values:
            return None

        return sorted(values)[-1]

    def extract_standard(self, df, sheet_name):

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

        metrics = {
            "total_assets": ["total assets"],
            "revenue": ["total income", "operating income", "net interest income"],
            "net_profit": ["net profit", "profit for the year", "profit attributable"],
            "operating_income": ["profit before tax"]
        }

        for metric, keywords in metrics.items():

            if metric == "revenue" and not any(x in sheet_lower for x in ["income", "pl", "comprehensive"]):
                continue

            value = self.find_metric_value(df, keywords)

            if value:
                results[metric] = value
                self.log(f"{sheet_name}: {metric} → {value}")

        return {year: results} if results else {}

    # =========================================================
    def process_excel(self, file_path):

        self.log(f"\n📄 Processing: {file_path}")

        try:

            # 🔥 SPECIAL CASE
            if "bangkok" in file_path.lower():
                return self.extract_bangkok_special(file_path)

            all_results = {}

            xls = pd.ExcelFile(file_path)

            for sheet in xls.sheet_names:

                df = xls.parse(sheet)

                extracted = self.extract_standard(df, sheet)

                for year, metrics in extracted.items():
                    all_results.setdefault(year, {})
                    all_results[year].update(metrics)

            return all_results

        except Exception as e:
            self.warn(f"File error: {e}")
            return {}

    # =========================================================
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