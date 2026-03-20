import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (ULTIMATE PRODUCTION) LOADED 🔥")

# 🔥 last 5 years only
CURRENT_YEAR = 2026
VALID_YEARS = [CURRENT_YEAR - i for i in range(5)]


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

            if np and eq and eq > 0:
                return round((np / eq) * 100, 2)
        except:
            pass
        return None

    # -------------------------------
    # SMART MERGE
    # -------------------------------
    def merge_metrics(self, existing, new):

        for k, v in new.items():

            if not v:
                continue

            if k not in existing:
                existing[k] = v
            else:
                if k in ["revenue", "net_profit", "equity", "total_assets"]:
                    existing[k] = max(existing[k], v)
                else:
                    existing[k] = v

        return existing

    # -------------------------------
    # FILL MISSING
    # -------------------------------
    def fill_missing(self, metrics):

        if not metrics.get("operating_income") and metrics.get("net_profit"):
            metrics["operating_income"] = metrics["net_profit"]

        return metrics

    # -------------------------------
    # CORE EXTRACTION
    # -------------------------------
    def extract_from_df(self, df, sheet_name):

        if df.empty:
            return {}

        df = df.fillna("")
        df.columns = range(len(df.columns))

        sheet_lower = sheet_name.lower()

        if any(x in sheet_lower for x in ["cash", "cf"]):
            return {}

        # detect valid year columns
        year_cols = {}

        for col in df.columns:
            for val in df[col].astype(str):
                match = re.search(r"(20\d{2})", val)
                if match:
                    year = int(match.group(1))
                    if year in VALID_YEARS:
                        year_cols[col] = year

        if not year_cols:
            return {}

        temp_results = {}

        keyword_map = {
            "revenue": ["total operating income", "total income"],
            "net_profit": ["net profit", "profit for the year", "profit attributable"],
            "operating_income": ["profit before tax", "operating profit", "income before tax"],
            "total_assets": ["total assets"],
            "equity": ["total equity", "shareholders", "equity attributable"]
        }

        for i in range(len(df)):

            row = df.iloc[i]
            row_text = " ".join(row.astype(str)).lower()

            for metric, keywords in keyword_map.items():

                if any(k in row_text for k in keywords):

                    for col_idx, year in year_cols.items():

                        val = row[col_idx]
                        clean_val = self.clean_value(val)

                        if clean_val:
                            temp_results.setdefault(year, {})
                            temp_results[year].setdefault(metric, [])
                            temp_results[year][metric].append(clean_val)

                            self.log(f"{sheet_name}: {metric} → {clean_val} ({year})")

        # pick best values
        final_results = {}

        for year, metrics in temp_results.items():

            final_results[year] = {}

            for metric, values in metrics.items():

                if not values:
                    continue

                if metric in ["revenue", "net_profit", "equity", "total_assets"]:
                    final_val = max(values)
                else:
                    final_val = values[-1]

                final_results[year][metric] = final_val

        return final_results

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

                    all_results[year] = self.merge_metrics(
                        all_results[year],
                        metrics
                    )

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

                    metrics = self.fill_missing(metrics)
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