import re
import sqlite3
from decimal import Decimal, InvalidOperation

import pdfplumber

from scripts.db_cache import init_db

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class FinancialExtractor:
    METRIC_KEYS = ("revenue", "net_profit", "operating_income", "total_assets", "roe")

    LABELS = {
        "operating_income": [
            "total operating income",
            "operating income",
            "total income",
        ],
        "net_profit": [
            "profit to equity holders",
            "profit for the period",
            "profit for the year",
            "profit attributable",
            "net profit",
        ],
        "total_assets": [
            "total assets",
        ],
        "roe": [
            "return on equity",
            "roe",
        ],
    }

    def __init__(self):
        print("Loading Financial Metrics Extractor (direct PDF via pdfplumber)")

    @staticmethod
    def normalize_numeric_string(raw):
        if raw is None:
            return None
        s = str(raw).strip().replace(",", "").replace("%", "")
        if not s:
            return None
        try:
            d = Decimal(s)
        except InvalidOperation:
            return None
        if d == d.to_integral():
            return str(int(d))
        return format(d, "f").rstrip("0").rstrip(".") or "0"

    @staticmethod
    def to_decimal(s):
        try:
            return Decimal(s)
        except (InvalidOperation, TypeError):
            return None

    def extract_numbers(self, line):
        nums = re.findall(r"\d{1,3}(?:,\d{3})+", line or "")
        out = []
        for n in nums:
            normalized = self.normalize_numeric_string(n)
            if normalized is None:
                continue
            d = self.to_decimal(normalized)
            if d is None:
                continue
            # Keep your anti-junk threshold
            if Decimal("1000") < d < Decimal("1000000000000"):
                out.append(normalized)
        return out

    def get_latest_value(self, line, key):
        values = self.extract_numbers(line)
        if not values:
            return None

        dec_values = [self.to_decimal(v) for v in values]
        dec_values = [v for v in dec_values if v is not None]
        if not dec_values:
            return None

        if key == "operating_income":
            filtered = [v for v in dec_values if v > Decimal("50000")]
            if filtered:
                return self.normalize_numeric_string(filtered[0])

        filtered = [v for v in dec_values if v > Decimal("10000")]
        if filtered:
            return self.normalize_numeric_string(filtered[0])
        return None

    def extract_financials(self, pdf_path):
        results = {
            "revenue": None,
            "net_profit": None,
            "operating_income": None,
            "total_assets": None,
            "roe": None,
        }

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                lines = text.split("\n")

                for line in lines:
                    line_lower = line.lower()

                    for metric_key, keywords in self.LABELS.items():
                        if results[metric_key] is not None:
                            continue
                        if not any(k in line_lower for k in keywords):
                            continue

                        if metric_key == "roe":
                            match = re.search(r"(\d+\.\d+|\d+)", line)
                            if match:
                                roe_val = self.normalize_numeric_string(match.group(1))
                                d = self.to_decimal(roe_val)
                                if d is not None and Decimal("0") <= d <= Decimal("100"):
                                    results[metric_key] = roe_val
                        else:
                            val = self.get_latest_value(line, metric_key)
                            if val is not None:
                                results[metric_key] = val

                if all(results[k] is not None for k in ("operating_income", "net_profit", "total_assets")):
                    break

        return results

    @staticmethod
    def infer_bank_name(file_path):
        parts = (file_path or "").replace("\\", "/").split("/")
        if len(parts) >= 3:
            return parts[-3].replace("_", " ")
        return "unknown_bank"

    def run(self):
        print("\nStarting direct PDF financial extraction\n")
        init_db()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT file_path, year
            FROM pdf_cache
            WHERE file_path IS NOT NULL
              AND TRIM(file_path) != ''
            """
        )
        rows = cursor.fetchall()

        if not rows:
            print("[INFO] No rows in pdf_cache.")
            conn.close()
            return

        print(f"[INFO] PDF files to process: {len(rows)}")

        saved = 0
        skipped = 0

        for file_path, year in rows:
            bank_name = self.infer_bank_name(file_path)
            print(f"\n[PDF] {bank_name} | {year} | {file_path}")

            try:
                metrics = self.extract_financials(file_path)
            except Exception as e:
                print(f"[ERROR] Failed to parse PDF: {e}")
                skipped += 1
                continue

            print(f"[PARSED] {metrics}")

            if not any(metrics.values()):
                print("[SKIP] No financial metric found in this PDF.")
                skipped += 1
                continue

            cursor.execute(
                """
                INSERT OR REPLACE INTO financial_metrics
                (bank_name, year, revenue, net_profit, operating_income, total_assets, roe)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bank_name,
                    int(year),
                    metrics.get("revenue"),
                    metrics.get("net_profit"),
                    metrics.get("operating_income"),
                    metrics.get("total_assets"),
                    metrics.get("roe"),
                ),
            )
            saved += 1
            print("[DB] Upserted into financial_metrics")

        conn.commit()
        conn.close()

        print("\n--- Summary ---")
        print(f"[SUMMARY] Upserted rows: {saved}")
        print(f"[SUMMARY] Skipped files: {skipped}")


def main():
    FinancialExtractor().run()
