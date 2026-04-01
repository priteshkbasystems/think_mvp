import os
import re
import sqlite3
import time
import json
from decimal import Decimal, InvalidOperation

import pdfplumber

from scripts.db_cache import init_db

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


class FinancialExtractor:
    METRIC_KEYS = ("revenue", "net_profit", "operating_income", "total_assets", "roe")

    LABELS = {
        "revenue": [
            "total operating income - net",
            "total operating income",
            "total operating revenue",
            "total revenue",
            "total income",
            "operating income",
        ],
        "operating_income": [
            "total operating income",
            "total operating income - net",
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
        "total_equity": [
            "total equity",
            "shareholders’ equity",
            "shareholders' equity",
            "equity attributable to owners",
            "equity attributable to equity holders",
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

    @staticmethod
    def detect_currency_and_unit(text):
        t = (text or "").lower()
        currency = None
        unit_multiplier = 1

        if "baht" in t or "thb" in t:
            currency = "THB"
        elif "usd" in t or "$" in t:
            currency = "USD"
        elif "eur" in t:
            currency = "EUR"

        if re.search(r"billion\s+baht", t):
            unit_multiplier = 1_000_000_000
        elif re.search(r"million\s+baht", t):
            unit_multiplier = 1_000_000
        elif re.search(r"thousand\s+baht|baht\s*[:']\s*'?\s*000|baht\s*/\s*'?\s*000", t):
            unit_multiplier = 1_000

        return currency, unit_multiplier

    def apply_unit_multiplier(self, value_str, unit_multiplier):
        if value_str is None:
            return None
        d = self.to_decimal(value_str)
        if d is None:
            return None
        return self.normalize_numeric_string(d * Decimal(unit_multiplier))

    @staticmethod
    def has_keyword(line_lower, keyword):
        escaped = re.escape(keyword.lower())
        pattern = r"(?<![a-z0-9])" + escaped + r"(?![a-z0-9])"
        return re.search(pattern, line_lower) is not None

    def extract_financials(self, pdf_path):
        results = {
            "revenue": None,
            "net_profit": None,
            "operating_income": None,
            "total_assets": None,
            "roe": None,
            "total_equity": None,
            "currency": None,
            "unit_multiplier": 1,
        }

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                if results["currency"] is None or results["unit_multiplier"] == 1:
                    ccy, unit = self.detect_currency_and_unit(text)
                    if results["currency"] is None and ccy is not None:
                        results["currency"] = ccy
                    if unit != 1:
                        results["unit_multiplier"] = unit
                lines = text.split("\n")

                for idx, line in enumerate(lines):
                    line_lower = line.lower()

                    for metric_key, keywords in self.LABELS.items():
                        if results[metric_key] is not None:
                            continue
                        if not any(self.has_keyword(line_lower, k) for k in keywords):
                            continue

                        if metric_key == "roe":
                            next_line = lines[idx + 1] if idx + 1 < len(lines) else ""
                            prev_line = lines[idx - 1] if idx - 1 >= 0 else ""
                            roe_text = f"{prev_line} {line} {next_line}"

                            # Prefer explicit percentages first.
                            percent_matches = re.findall(r"(\d{1,3}(?:\.\d+)?)\s*%", roe_text)
                            candidates = percent_matches or re.findall(r"(\d{1,3}(?:\.\d+)?)", roe_text)

                            for c in candidates:
                                roe_val = self.normalize_numeric_string(c)
                                d = self.to_decimal(roe_val)
                                if d is not None and Decimal("0") < d <= Decimal("100"):
                                    results[metric_key] = roe_val
                                    break
                        else:
                            val = self.get_latest_value(line, metric_key)
                            if val is not None:
                                results[metric_key] = val

                if all(results[k] is not None for k in ("net_profit", "total_assets")):
                    break

        # normalize monetary values by detected report unit
        for k in ("revenue", "net_profit", "operating_income", "total_assets", "total_equity"):
            results[k] = self.apply_unit_multiplier(results.get(k), results["unit_multiplier"])

        if results["roe"] is None:
            np_val = self.to_decimal(results.get("net_profit"))
            eq_val = self.to_decimal(results.get("total_equity"))
            if np_val is not None and eq_val is not None and eq_val != 0:
                roe_calc = (np_val / eq_val) * Decimal("100")
                results["roe"] = self.normalize_numeric_string(roe_calc.quantize(Decimal("0.01")))

        if results["revenue"] is None and results["operating_income"] is not None:
            results["revenue"] = results["operating_income"]

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

        conn = sqlite3.connect(DB_PATH, timeout=60)
        cursor = conn.cursor()
        cursor.execute("PRAGMA busy_timeout = 60000")
        cursor.execute("PRAGMA journal_mode = DELETE")
        cursor.execute("PRAGMA synchronous = NORMAL")

        # Keep quarterly rows separately so they don't overwrite annual by (bank_name, year) PK.
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS financial_metrics_periodic (
                bank_id INTEGER,
                bank_name TEXT,
                year INTEGER,
                period_type TEXT,
                period_label TEXT,
                currency TEXT,
                unit_multiplier INTEGER,
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
        # lightweight migrations for existing DBs
        cursor.execute("PRAGMA table_info(financial_metrics_periodic)")
        periodic_cols = {row[1] for row in cursor.fetchall()}
        if "currency" not in periodic_cols:
            cursor.execute("ALTER TABLE financial_metrics_periodic ADD COLUMN currency TEXT")
        if "unit_multiplier" not in periodic_cols:
            cursor.execute("ALTER TABLE financial_metrics_periodic ADD COLUMN unit_multiplier INTEGER")
        if "bank_id" not in periodic_cols:
            cursor.execute("ALTER TABLE financial_metrics_periodic ADD COLUMN bank_id INTEGER")

        cursor.execute("PRAGMA table_info(financial_metrics)")
        annual_cols = {row[1] for row in cursor.fetchall()}
        if "currency" not in annual_cols:
            cursor.execute("ALTER TABLE financial_metrics ADD COLUMN currency TEXT")
        if "unit_multiplier" not in annual_cols:
            cursor.execute("ALTER TABLE financial_metrics ADD COLUMN unit_multiplier INTEGER")

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
            cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
            bank_id_row = cursor.fetchone()
            bank_id = bank_id_row[0] if bank_id_row else None

            try:
                metrics = self.extract_financials(file_path)
            except Exception as e:
                print(f"[ERROR] Failed to parse PDF: {e}")
                skipped += 1
                continue

            print(
                json.dumps(
                    {
                        "bank_name": bank_name,
                        "currency": metrics.get("currency"),
                        "unit_multiplier": metrics.get("unit_multiplier"),
                        "metrics": {
                            "Total Operating Income": metrics.get("operating_income"),
                            "Net Profit": metrics.get("net_profit"),
                            "Total Assets": metrics.get("total_assets"),
                            "ROE (%)": metrics.get("roe"),
                        },
                    },
                    ensure_ascii=True,
                )
            )

            if not any(metrics.values()):
                print("[SKIP] No financial metric found in this PDF.")
                skipped += 1
                continue

            # Always keep period-specific record (annual + quarterly)
            cursor.execute(
                """
                INSERT OR REPLACE INTO financial_metrics_periodic
                (bank_id, bank_name, year, period_type, period_label, currency, unit_multiplier, revenue, net_profit, operating_income, total_assets, roe, source_file_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bank_id,
                    bank_name,
                    int(year),
                    period_type,
                    period_label,
                    metrics.get("currency"),
                    metrics.get("unit_multiplier"),
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
                    (bank_id, bank_name, year, currency, unit_multiplier, revenue, net_profit, operating_income, total_assets, roe)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bank_id,
                        bank_name,
                        int(year),
                        metrics.get("currency"),
                        metrics.get("unit_multiplier"),
                        metrics.get("revenue"),
                        metrics.get("net_profit"),
                        metrics.get("operating_income"),
                        metrics.get("total_assets"),
                        metrics.get("roe"),
                    ),
                )
                saved += 1
                print("[DB] Upserted into financial_metrics (annual)")

        commit_ok = False
        for attempt in range(1, 6):
            try:
                conn.commit()
                commit_ok = True
                break
            except sqlite3.OperationalError as e:
                print(f"[WARN] Commit failed (attempt {attempt}/5): {e}")
                if "disk i/o error" not in str(e).lower() and "disk I/O error" not in str(e):
                    break
                time.sleep(attempt * 2)

        conn.close()

        if not commit_ok:
            print("[ERROR] Could not commit DB changes due to disk I/O error.")
            return

        print("\n--- Summary ---")
        print(f"[SUMMARY] Upserted annual rows (financial_metrics): {saved}")
        print(f"[SUMMARY] Upserted periodic rows (financial_metrics_periodic): {saved_periodic}")
        print(f"[SUMMARY] Skipped files: {skipped}")


def main():
    FinancialExtractor().run()
