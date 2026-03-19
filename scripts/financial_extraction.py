import os
import re
import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 Loading Financial Metrics Extractor (Excel Based)\n")

        self.metric_keywords = {
            "revenue": ["revenue", "total revenue", "income"],
            "net_profit": ["net profit", "net income", "profit after tax"],
            "operating_income": ["operating income", "operating profit"],
            "total_assets": ["total assets"],
            "roe": ["roe", "return on equity"]
        }

    # -------------------------------
    def log(self, message):
        print(f"[LOG] {message}")

    def warn(self, message):
        print(f"[WARNING] {message}")

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

        self.log(f"{sheet_name}: Detected years → {[y for _, y in year_cols]}")

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

            self.log(f"Sheets found: {xls.sheet_names}")

            for sheet in xls.sheet_names:
                try:
                    df = xls.parse(sheet)
                    extracted = self.extract_from_df(df, sheet)

                    if not extracted:
                        self.warn(f"{sheet}: No financial data extracted")

                    for year, metrics in extracted.items():
                        if year not in all_results:
                            all_results[year] = {}

                        all_results[year].update(metrics)

                except Exception as e:
                    self.warn(f"Sheet error ({sheet}): {e}")

        except Exception as e:
            self.warn(f"File error: {file_path} → {e}")

        self.log(f"📊 Final extracted data: {all_results}\n")

        return all_results

    # -------------------------------
    def run(self):

        self.log("Connecting to database...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT bank_name FROM banks")
        banks = cursor.fetchall()

        for (bank,) in banks:

            self.log(f"\n🏦 Processing Bank: {bank}")

            base_path = f"/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/{bank}/financial_report"

            if not os.path.exists(base_path):
                self.warn(f"{bank}: financial_report folder not found")
                continue

            files = [f for f in os.listdir(base_path) if f.endswith((".xlsx", ".xls"))]

            if not files:
                self.warn(f"{bank}: No Excel files found")
                continue

            for file in files:

                file_path = os.path.join(base_path, file)

                results = self.process_excel(file_path)

                if not results:
                    self.warn(f"{bank}: No metrics extracted from {file}")
                    continue

                for year, metrics in results.items():

                    self.log(f"💾 Saving → {bank} | {year} | {metrics}")

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

        self.log("\n✅ Financial metrics extraction completed.\n")


# -------------------------------
# ENTRY POINT (IMPORTANT)
# -------------------------------
def main():
    extractor = FinancialExtractor()
    extractor.run()