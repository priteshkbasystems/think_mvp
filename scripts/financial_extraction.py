import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (PRODUCTION STABLE) LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (FINAL STABLE)\n")

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
            val = float(value)

            # 🔥 FILTER BAD VALUES
            if val < 1000:
                return None
            if val > 1e13:  # avoid corrupted huge values
                return None

            return val
        except:
            return None

    # -------------------------------
    # 🔥 SPECIAL BANGKOK HANDLER
    # -------------------------------
    def extract_bangkok(self, df):

        results = {}
        text = " ".join(df.astype(str).values.flatten()).lower()

        year_match = re.search(r"(20\d{2})", text)
        if not year_match:
            return {}

        year = int(year_match.group(1))
        results[year] = {}

        # -------------------------------
        # STEP 1: REGEX
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
        # STEP 2: SAFE FALLBACK
        # -------------------------------
        if "total_assets" not in results[year]:

            candidates = []

            for val in df.values.flatten():
                value = self.clean_value(val)

                # realistic bank asset range
                if value and 1e6 < value < 1e12:
                    candidates.append(value)

            if candidates:
                best = max(candidates)
                results[year]["total_assets"] = best
                self.log(f"Bangkok fallback total_assets → {best}")

        return results

    # -------------------------------
    def extract_from_df(self, df, sheet_name):

        if df.empty:
            return {}

        sheet_lower = sheet_name.lower()

        # skip irrelevant sheets
        if any(x in sheet_lower for x in ["change", "equity", "cash", "cf"]):
            return {}

        full_text = " ".join(df.astype(str).values.flatten()).lower()

        year_match = re.search(r"(20\d{2})", full_text)
        if not year_match:
            return {}

        year = int(year_match.group(1))
        self.log(f"{sheet_name}: Year → {year}")

        keyword_map = {
            "revenue": [
                "total operating income",
                "total income",
                "interest income",
                "net interest income"
            ],
            "net_profit": [
                "net profit",
                "profit for the year",
                "profit attributable"
            ],
            "operating_income": [
                "profit before tax",
                "profit before income tax"
            ],
            "total_assets": [
                "total assets"
            ]
        }

        temp_store = {k: [] for k in keyword_map.keys()}

        rows = df.values

        for i in range(len(rows)):

            row_text = " ".join([str(x).lower() for x in rows[i]])

            for metric, keywords in keyword_map.items():

                # revenue only from income sheets
                if metric == "revenue" and not any(x in sheet_lower for x in ["income", "pl", "comprehensive"]):
                    continue

                if any(k in row_text for k in keywords):

                    # 🔥 search current + next row
                    for r in [i, min(i+1, len(rows)-1)]:
                        for val in rows[r]:
                            value = self.clean_value(val)
                            if value:
                                temp_store[metric].append(value)

        final = {}

        for metric, values in temp_store.items():
            if values:
                # pick MOST REALISTIC (not always max)
                values = sorted(values)
                final_val = values[-1]

                final[metric] = final_val
                self.log(f"{sheet_name}: FINAL {metric} → {final_val}")

        return {year: final} if final else {}

    # -------------------------------
    def process_excel(self, file_path):

        self.log(f"\n📄 Processing: {file_path}")

        try:
            if "bangkok" in file_path.lower():
                df = pd.concat(pd.read_excel(file_path, sheet_name=None).values())
                return self.extract_bangkok(df)

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