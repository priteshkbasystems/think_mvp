import re
import sqlite3
from scripts.transformation_correlation import extract_text_from_pdf

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class FinancialExtractor:

    def __init__(self):

        print("Loading Financial Metrics Extractor")

    def extract_metrics(self, text):

        metrics = {}

        patterns = {
            "revenue": r"revenue[^0-9]{0,10}([\d,]+)",
            "net_profit": r"net profit[^0-9]{0,10}([\d,]+)",
            "operating_income": r"operating income[^0-9]{0,10}([\d,]+)",
            "total_assets": r"total assets[^0-9]{0,10}([\d,]+)",
            "roe": r"return on equity[^0-9]{0,10}([\d\.]+)"
        }

        text = text.lower()

        for key, pattern in patterns.items():

            match = re.search(pattern, text)

            if match:

                value = match.group(1).replace(",", "")

                try:
                    metrics[key] = float(value)
                except:
                    pass

        return metrics

    def run(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT file_path, year FROM pdf_cache")

        rows = cursor.fetchall()

        for path, year in rows:

            bank = path.split("/")[-3]

            text = extract_text_from_pdf(path)

            metrics = self.extract_metrics(text)

            if not metrics:
                continue

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

        print("Financial metrics extraction completed.")