import os
import re
import sqlite3
from decimal import Decimal, InvalidOperation

import pdfplumber

from scripts.db_cache import init_db

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


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

    @staticmethod
    def is_financial_report_pdf(file_path):
        norm = (file_path or "").replace("\\", "/").strip("/")
        parts = [p.lower() for p in norm.split("/") if p]
        if len(parts) < 2:
            return False
        # strict: immediate parent folder must be exactly "financial_report"
        if parts[-2] != "financial_report":
            return False
        # extra safety: reject known non-target folders anywhere in path
        blocked = {"annual_reports", "investor_presentations", "reviews", "stock_price"}
        return not any(p in blocked for p in parts)

    @staticmethod
    def infer_period(file_path):
        """
        Returns: (period_type, period_label)
        period_type: annual | quarterly
        period_label: FY / Q1 / Q2 / Q3 / Q4
        """
        name = ((file_path or "").replace("\\", "/").split("/")[-1]).lower()

        if re.search(r"\bq1\b|quarter\s*1|1q|1st\s*quarter", name):
            return "quarterly", "Q1"
        if re.search(r"\bq2\b|quarter\s*2|2q|2nd\s*quarter", name):
            return "quarterly", "Q2"
        if re.search(r"\bq3\b|quarter\s*3|3q|3rd\s*quarter", name):
            return "quarterly", "Q3"
        if re.search(r"\bq4\b|quarter\s*4|4q|4th\s*quarter", name):
            return "quarterly", "Q4"

        return "annual", "FY"

    @staticmethod
    def infer_year(file_name):
        match = re.search(r"(20\d{2})", file_name or "")
        if match:
            return int(match.group(1))
        return None

    def list_financial_report_pdfs(self):
        rows = []
        if not os.path.isdir(BASE_CORP_PATH):
            return rows

        for bank_folder in sorted(os.listdir(BASE_CORP_PATH)):
            bank_path = os.path.join(BASE_CORP_PATH, bank_folder)
            if not os.path.isdir(bank_path):
                continue

            fin_path = os.path.join(bank_path, "financial_report")
            if not os.path.isdir(fin_path):
                continue

            bank_name = bank_folder.replace("_", " ")
            for root, _, files in os.walk(fin_path):
                for file_name in sorted(files):
                    if not file_name.lower().endswith(".pdf"):
                        continue
                    if file_name.startswith("~$"):
                        continue

                    rel_path = os.path.join(root, file_name)
                    year = self.infer_year(file_name) or self.infer_year(rel_path)
                    if year is None:
                        continue

                    period_type, period_label = self.infer_period(file_name)
                    rows.append((bank_name, year, period_type, period_label, rel_path))

        return rows

    def run(self):
        print("\nStarting direct PDF financial extraction\n")
        init_db()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Keep quarterly rows separately so they don't overwrite annual by (bank_name, year) PK.
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS financial_metrics_periodic (
                bank_name TEXT,
                year INTEGER,
                period_type TEXT,
                period_label TEXT,
                revenue TEXT,
                net_profit TEXT,
                operating_income TEXT,
                total_assets TEXT,
                roe TEXT,
                source_file_path TEXT,
                PRIMARY KEY(bank_name, year, period_type, period_label)
            )
            """
        )

        rows = self.list_financial_report_pdfs()

        if not rows:
            print("[INFO] No PDFs found under */financial_report folders.")
            conn.close()
            return

        print(f"[INFO] financial_report PDFs to process: {len(rows)}")

        saved = 0
        saved_periodic = 0
        skipped = 0

        for bank_name, year, period_type, period_label, file_path in rows:
            print(f"\n[PDF] {bank_name} | {year} | {period_label} | {file_path}")

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

            # Always keep period-specific record (annual + quarterly)
            cursor.execute(
                """
                INSERT OR REPLACE INTO financial_metrics_periodic
                (bank_name, year, period_type, period_label, revenue, net_profit, operating_income, total_assets, roe, source_file_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bank_name,
                    int(year),
                    period_type,
                    period_label,
                    metrics.get("revenue"),
                    metrics.get("net_profit"),
                    metrics.get("operating_income"),
                    metrics.get("total_assets"),
                    metrics.get("roe"),
                    file_path,
                ),
            )
            saved_periodic += 1
            print("[DB] Upserted into financial_metrics_periodic")

            # Keep existing table behavior for annual only
            if period_type == "annual":
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
                print("[DB] Upserted into financial_metrics (annual)")

        conn.commit()
        conn.close()

        print("\n--- Summary ---")
        print(f"[SUMMARY] Upserted annual rows (financial_metrics): {saved}")
        print(f"[SUMMARY] Upserted periodic rows (financial_metrics_periodic): {saved_periodic}")
        print(f"[SUMMARY] Skipped files: {skipped}")


def main():
    FinancialExtractor().run()
