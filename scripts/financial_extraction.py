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
        print("\n🚀 Financial Metrics Extractor (SMART MODE)\n")

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

    def find_bank_folder(self, bank_name):
        for folder in os.listdir(BASE_PATH):
            if self.normalize_name(folder) == self.normalize_name(bank_name):
                return os.path.join(BASE_PATH, folder)
        return None

    # -------------------------------
    def clean_value(self, value):
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
    def match_metric(self, text):
        text = str(text).lower()
        for metric, keywords in self.metric_keywords.items():
            for keyword in keywords:
                if keyword in text:
                    return metric
        return None

    # -------------------------------
    def extract_from_df(self, df, sheet_name):
        results = {}

        if df.empty:
            return results

        df = df.fillna("")

        # ===============================
        # 1. COLUMN BASED
        # ===============================
        year_cols = self.extract_years(df.columns)

        if year_cols:
            self.log(f"{sheet_name}: Column-based years → {[y for _, y in year_cols]}")

            first_col = df.columns[0]

            for _, row in df.iterrows():
                metric = self.match_metric(row[first_col])
                if not metric:
                    continue

                for col, year in year_cols:
                    value = self.clean_value(row[col])
                    if value is None:
                        continue

                    results.setdefault(year, {})
                    results[year][metric] = value

            if results:
                return results

        # ===============================
        # 2. ROW BASED (IMPORTANT)
        # ===============================
        self.log(f"{sheet_name}: Trying row-based extraction")

        detected_year = None

        # detect year
        for i in range(min(10, len(df))):
            row_text = " ".join([str(x) for x in df.iloc[i].values])
            match = re.search(r"(20\d{2})", row_text)
            if match:
                detected_year = int(match.group(1))
                self.log(f"{sheet_name}: Year detected → {detected_year}")
                break

        if not detected_year:
            return results

        for _, row in df.iterrows():
            row_text = " ".join([str(x).lower() for x in row.values])

            metric = self.match_metric(row_text)
            if not metric:
                continue

            for val in row.values:
                value = self.clean_value(val)
                if value is not None:
                    results.setdefault(detected_year, {})
                    results[detected_year][metric] = value
                    break

        if results:
            return results

        # ===============================
        # 3. FULL TEXT FALLBACK
        # ===============================
        self.log(f"{sheet_name}: Trying fallback extraction")

        full_text = " ".join(df.astype(str).values.flatten()).lower()

        year_match = re.search(r"(20\d{2})", full_text)
        if not year_match:
            return results

        year = int(year_match.group(1))

        for metric, keywords in self.metric_keywords.items():
            for keyword in keywords:
                pattern = rf"{keyword}[^0-9]{{0,20}}([\d,\.]+)"
                match = re.search(pattern, full_text)

                if match:
                    value = self.clean_value(match.group(1))
                    if value:
                        results.setdefault(year, {})
                        results[year][metric] = value
                        break

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


# -------------------------------
def main():
    FinancialExtractor().run()