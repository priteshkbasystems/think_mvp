import re
import sqlite3
from decimal import Decimal, InvalidOperation

from scripts.db_cache import init_db

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"


class FinancialExtractor:

    METRIC_KEYS = ("revenue", "net_profit", "operating_income", "total_assets", "roe")

    def __init__(self):
        print(
            "Loading Financial Metrics Extractor "
            "(corporate_sentence_sentiment + pdf_text_cache fallback; values stored as TEXT)"
        )

    @staticmethod
    def normalize_numeric_string(raw):
        """Plain digit string for SQLite — no float/scientific notation."""
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

    def passes_threshold(self, metric, storage_str):
        d = self.to_decimal(storage_str)
        if d is None:
            return False
        if metric == "roe":
            return Decimal(0) <= d <= Decimal(100)
        return d > Decimal(1000)

    def extract_metrics_from_sentence(self, sentence_text):
        text = (sentence_text or "").lower()
        if not text:
            return {}

        patterns = {
            "revenue": r"(net interest income|total operating income|total income|revenue)[^0-9]{0,40}([\d,]+(?:\.\d+)?)",
            "net_profit": r"(profit attributable|net profit|profit for the year)[^0-9]{0,40}([\d,]+(?:\.\d+)?)",
            "operating_income": r"(operating income|profit before tax|profit before income tax)[^0-9]{0,40}([\d,]+(?:\.\d+)?)",
            "total_assets": r"(total assets)[^0-9]{0,40}([\d,]+(?:\.\d+)?)",
            "roe": r"(return on equity|roe)[^0-9]{0,20}([\d,]+(?:\.\d+)?)",
        }

        found = {}
        for metric, pattern in patterns.items():
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue

            storage = self.normalize_numeric_string(match.group(2))
            if storage is None:
                continue
            if self.passes_threshold(metric, storage):
                found[metric] = storage

        keyword_map = {
            "revenue": ["net interest income", "total operating income", "total income", "revenue"],
            "net_profit": ["profit attributable", "net profit", "profit for the year"],
            "operating_income": ["operating income", "profit before tax", "profit before income tax"],
            "total_assets": ["total assets"],
            "roe": ["return on equity", "roe"],
        }
        tokens = re.findall(r"[\d,]+(?:\.\d+)?", text)
        all_strings = []
        for t in tokens:
            s = self.normalize_numeric_string(t)
            if s is not None:
                all_strings.append(s)

        for metric, keys in keyword_map.items():
            if metric in found:
                continue
            if not any(k in text for k in keys):
                continue
            if not all_strings:
                continue

            if metric == "roe":
                candidates = [
                    s for s in all_strings
                    if self.passes_threshold("roe", s)
                ]
                if candidates:
                    found[metric] = max(candidates, key=lambda x: Decimal(x))
            else:
                candidates = [
                    s for s in all_strings
                    if self.passes_threshold(metric, s)
                ]
                if candidates:
                    found[metric] = max(candidates, key=lambda x: Decimal(x))

        return found

    def extract_metrics_from_long_text(self, text):
        if not text or not str(text).strip():
            return {}
        buckets = {k: [] for k in self.METRIC_KEYS}
        for line in str(text).splitlines():
            line = line.strip()
            if len(line) < 8:
                continue
            found = self.extract_metrics_from_sentence(line)
            for k, v in found.items():
                buckets[k].append(v)
        out = {}
        for k, vs in buckets.items():
            if vs:
                out[k] = max(vs, key=lambda x: Decimal(x))
        return out

    def _merge_max(self, a, b):
        out = dict(a)
        for k, v in b.items():
            if v is None:
                continue
            if k not in out or out[k] is None:
                out[k] = v
            else:
                out[k] = max([out[k], v], key=lambda x: Decimal(x))
        return out

    def _needs_pdf_fallback(self, result):
        if not result:
            return True
        core = ("revenue", "net_profit", "total_assets")
        return any(result.get(k) is None for k in core)

    def _missing_keys(self, result):
        return [k for k in self.METRIC_KEYS if result.get(k) is None]

    def _log_section(self, title):
        print(f"\n--- {title} ---")

    def run(self):
        print("\nStarting extraction (sentences first, pdf_text_cache fallback)\n")

        init_db()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT bank_name, year, sentence_text
            FROM corporate_sentence_sentiment
            WHERE sentence_text IS NOT NULL
              AND TRIM(sentence_text) != ''
            """
        )
        rows = cursor.fetchall()

        if not rows:
            print("[INFO] No rows in corporate_sentence_sentiment (non-empty sentence_text). Stop.")
            conn.close()
            return

        print(f"[INFO] corporate_sentence_sentiment rows to scan: {len(rows)}")

        grouped = {}
        file_paths_by_key = {}
        for bank_name, year, sentence_text in rows:
            key = (bank_name, int(year))
            grouped.setdefault(
                key,
                {
                    "revenue": [],
                    "net_profit": [],
                    "operating_income": [],
                    "total_assets": [],
                    "roe": [],
                },
            )
            extracted = self.extract_metrics_from_sentence(sentence_text)
            for metric, value in extracted.items():
                grouped[key][metric].append(value)

        cursor.execute(
            """
            SELECT DISTINCT bank_name, year, file_path
            FROM corporate_sentence_sentiment
            WHERE file_path IS NOT NULL AND TRIM(file_path) != ''
            """
        )
        for bank_name, year, file_path in cursor.fetchall():
            key = (bank_name, int(year))
            file_paths_by_key.setdefault(key, set()).add(file_path)

        print(f"[INFO] Distinct (bank, year) groups: {len(grouped)}")
        print(f"[INFO] Distinct file_path keys for PDF fallback: {len(file_paths_by_key)}")

        saved = 0
        skipped_empty = 0
        used_pdf_fallback = 0

        for (bank_name, year), metric_values in sorted(grouped.items()):
            tag = f"{bank_name} | {year}"
            self._log_section(tag)

            result = {}
            for metric, values in metric_values.items():
                if not values:
                    continue
                result[metric] = max(values, key=lambda x: Decimal(x))

            print(
                f"[SENTENCES] Parsed from sentence_text → "
                f"{ {k: result[k] for k in self.METRIC_KEYS if k in result} or '(none)' }"
            )
            print(
                f"[SENTENCES] Missing after sentences: "
                f"{self._missing_keys(result) or '(none — all keys present or N/A)'} "
                f"| need PDF fallback (core): {self._needs_pdf_fallback(result)}"
            )

            if self._needs_pdf_fallback(result):
                paths = list(file_paths_by_key.get((bank_name, year), ()))
                if not paths:
                    print(
                        "[PDF] Skip fallback — no file_path in corporate_sentence_sentiment "
                        "for this bank/year (cannot look up pdf_text_cache)."
                    )
                else:
                    print(f"[PDF] Trying {len(paths)} cached PDF(s): {paths}")
                    for fp in paths:
                        cursor.execute(
                            "SELECT text FROM pdf_text_cache WHERE file_path = ?",
                            (fp,),
                        )
                        row = cursor.fetchone()
                        if not row or not row[0]:
                            print(f"  [PDF] MISS — no text in pdf_text_cache: {fp}")
                            continue
                        txt = row[0]
                        print(
                            f"  [PDF] HIT — text length={len(txt)} chars: {fp}"
                        )
                        pdf_part = self.extract_metrics_from_long_text(txt)
                        print(
                            f"  [PDF] Parsed from full text → "
                            f"{ {k: pdf_part[k] for k in self.METRIC_KEYS if k in pdf_part} or '(none)' }"
                        )
                        before = set(result.keys())
                        result = self._merge_max(result, pdf_part)
                        added = set(result.keys()) - before
                        if added:
                            print(f"  [PDF] New keys merged: {sorted(added)}")
                        used_pdf_fallback += 1
                        if not self._needs_pdf_fallback(result):
                            print("[PDF] Core metrics satisfied — stop trying more PDFs.")
                            break

            print(
                f"[FINAL] Values to save (TEXT): "
                f"{ {k: result.get(k) for k in self.METRIC_KEYS} }"
            )
            print(f"[FINAL] Still missing: {self._missing_keys(result) or '(none)'}")

            if not result:
                print("[SKIP] No numeric metrics extracted — not writing financial_metrics.")
                skipped_empty += 1
                continue

            print(f"[DB] INSERT OR REPLACE financial_metrics for {tag}")
            cursor.execute(
                """
                INSERT OR REPLACE INTO financial_metrics
                (bank_name, year, revenue, net_profit, operating_income, total_assets, roe)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bank_name,
                    year,
                    result.get("revenue"),
                    result.get("net_profit"),
                    result.get("operating_income"),
                    result.get("total_assets"),
                    result.get("roe"),
                ),
            )
            saved += 1

        conn.commit()
        conn.close()
        print("\n--- Summary ---")
        print(f"[SUMMARY] financial_metrics rows upserted: {saved}")
        print(f"[SUMMARY] Groups skipped (no metrics at all): {skipped_empty}")
        print(f"[SUMMARY] PDF cache lookups that returned text (at least once): {used_pdf_fallback}")
        print("")


def main():
    FinancialExtractor().run()
