import os
import re
import sqlite3
import pandas as pd
import sys

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (AI-LEVEL 100%) LOADED 🔥")

CURRENT_YEAR = 2026
VALID_YEARS = [CURRENT_YEAR - i for i in range(5)]


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Financial Metrics Extractor (AI ENGINE)\n")

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
    # MERGE
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
    # STRUCTURE EXTRACTION
    # -------------------------------
    def extract_from_df(self, df, sheet_name):

        if df.empty:
            return {}

        df = df.fillna("")
        df.columns = range(len(df.columns))

        if any(x in sheet_name.lower() for x in ["cash", "cf"]):
            return {}

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

        keyword_map = {
            "revenue": ["total operating income", "total income"],
            "net_profit": ["net profit", "profit for the year", "profit attributable"],
            "operating_income": ["profit before tax", "operating profit"],
            "total_assets": ["total assets"],
            "equity": ["total equity", "shareholders"]
        }

        temp = {}

        for i in range(len(df)):
            row = df.iloc[i]
            row_text = " ".join(row.astype(str)).lower()

            for metric, keys in keyword_map.items():

                if any(k in row_text for k in keys):

                    for col_idx, year in year_cols.items():
                        val = self.clean_value(row[col_idx])

                        if val:
                            temp.setdefault(year, {})
                            temp[year].setdefault(metric, []).append(val)

        final = {}

        for year, metrics in temp.items():
            final[year] = {}

            for k, vals in metrics.items():
                final[year][k] = max(vals)

        return final

    # -------------------------------
    # TEXT FALLBACK
    # -------------------------------
    def fallback_text(self, df):

        text = " ".join(df.astype(str).values.flatten()).lower()

        data = {}

        patterns = {
            "revenue": r"total operating income[^0-9]{0,50}([\d,]+)",
            "net_profit": r"(net profit|profit attributable)[^0-9]{0,50}([\d,]+)",
            "total_assets": r"total assets[^0-9]{0,50}([\d,]+)",
            "equity": r"(total equity|shareholders)[^0-9]{0,50}([\d,]+)"
        }

        for k, p in patterns.items():
            m = re.search(p, text)
            if m:
                val = self.clean_value(m.group(1))
                if val:
                    data[k] = val

        return data

    # -------------------------------
    # BANGKOK SPECIAL
    # -------------------------------
    def extract_bangkok(self, file_path):

        df = pd.concat(pd.read_excel(file_path, sheet_name=None).values())
        text = " ".join(df.astype(str).values.flatten()).lower()

        year = 2025

        return {
            year: self.fallback_text(df)
        }

    # -------------------------------
    # AI FALLBACK (HOOK)
    # -------------------------------
    def ai_fallback(self, df):
        # 🔥 placeholder for GPT / LLM integration
        # you can plug OpenAI here later
        return {}

    # -------------------------------
    def process_excel(self, file_path):

        self.log(f"\n📄 Processing: {file_path}")

        try:

            if "bangkok" in file_path.lower():
                return self.extract_bangkok(file_path)

            xls = pd.ExcelFile(file_path)
            all_results = {}

            for sheet in xls.sheet_names:

                df = xls.parse(sheet)

                structured = self.extract_from_df(df, sheet)

                for year, metrics in structured.items():
                    all_results.setdefault(year, {})
                    all_results[year] = self.merge_metrics(all_results[year], metrics)

                fallback = self.fallback_text(df)

                for year in all_results:
                    all_results[year] = self.merge_metrics(all_results[year], fallback)

                # 🔥 AI fallback (last layer)
                ai_data = self.ai_fallback(df)

                for year in all_results:
                    all_results[year] = self.merge_metrics(all_results[year], ai_data)

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

        for (bank,) in banks:

            print(f"\n🏦 {bank}")

            folder = self.find_bank_folder(bank)

            if not folder:
                continue

            path = os.path.join(folder, "financial_report")

            if not os.path.exists(path):
                continue

            for file in os.listdir(path):

                if not file.lower().endswith((".xlsx", ".xls")):
                    continue

                file_path = os.path.join(path, file)

                results = self.process_excel(file_path)

                for year, metrics in results.items():

                    metrics = self.fill_missing(metrics)
                    metrics["roe"] = self.compute_roe(metrics)

                    print(f"💾 {bank} | {year} | {metrics}")

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