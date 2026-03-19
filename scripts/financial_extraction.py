import os
import re
import sqlite3
import pandas as pd

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class FinancialExtractor:

    def __init__(self):
        print("Loading Financial Metrics Extractor (Excel Based)")

        # Flexible keyword mapping (VERY IMPORTANT)
        self.metric_keywords = {
            "revenue": ["revenue", "total revenue", "income"],
            "net_profit": ["net profit", "net income", "profit after tax"],
            "operating_income": ["operating income", "operating profit"],
            "total_assets": ["total assets"],
            "roe": ["roe", "return on equity"]
        }

    # -------------------------------
    # Utility: Clean numeric values
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
    # Detect year columns
    # -------------------------------
    def extract_years(self, columns):
        years = []
        for col in columns:
            match = re.search(r"(20\d{2})", str(col))
            if match:
                years.append((col, int(match.group(1))))
        return years

    # -------------------------------
    # Match row name with metric
    # -------------------------------
    def match_metric(self, row_name):
        row_name = str(row_name).lower()

        for metric, keywords in self.metric_keywords.items():
            for keyword in keywords:
                if keyword in row_name:
                    return metric
        return None

    # -------------------------------
    # Extract metrics from dataframe
    # -------------------------------
    def extract_from_df(self, df):
        results = {}

        if df.empty:
            return results

        # Assume first column contains labels
        df = df.dropna(how="all")

        first_col = df.columns[0]

        year_cols = self.extract_years(df.columns)

        if not year_cols:
            return results

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

        return results

    # -------------------------------
    # Process Excel file
    # -------------------------------
    def process_excel(self, file_path):
        all_results = {}

        try:
            xls = pd.ExcelFile(file_path)

            for sheet in xls.sheet_names:
                try:
                    df = xls.parse(sheet)
                    extracted = self.extract_from_df(df)

                    for year, metrics in extracted.items():
                        if year not in all_results:
                            all_results[year] = {}

                        all_results[year].update(metrics)

                except Exception as e:
                    print(f"Sheet error ({sheet}): {e}")

        except Exception as e:
            print(f"File error: {file_path} → {e}")

        return all_results

    # -------------------------------
    # MAIN RUN
    # -------------------------------
    def run(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Get all banks
        cursor.execute("SELECT bank_name FROM banks")
        banks = cursor.fetchall()

        for (bank,) in banks:

            base_path = f"/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/{bank}/financial_report"

            if not os.path.exists(base_path):
                continue

            files = [f for f in os.listdir(base_path) if f.endswith((".xlsx", ".xls"))]

            for file in files:

                file_path = os.path.join(base_path, file)

                print(f"Processing: {file_path}")

                results = self.process_excel(file_path)

                for year, metrics in results.items():

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

        print("✅ Financial metrics extraction from Excel completed.")