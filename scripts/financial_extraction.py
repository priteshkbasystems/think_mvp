import os
import re
import sqlite3
import pandas as pd
import sys
import pdfplumber
from datetime import datetime

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 ENTERPRISE AI EXTRACTOR + VALIDATION LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 AI ENGINE WITH VALIDATION + ERROR DETECTION\n")

    # -------------------------------
    def clean_value(self, v):
        try:
            v = str(v).replace(",", "").replace("%", "").strip()
            return float(v)
        except:
            return None

    # -------------------------------
    def log_issue(self, cursor, bank, year, msg, severity="warning"):
        cursor.execute("""
        INSERT INTO extraction_logs (bank_name, year, issue, severity)
        VALUES (?, ?, ?, ?)
        """, (bank, year, msg, severity))

    # -------------------------------
    def validate_data(self, data):

        issues = []
        score = 100

        # critical checks
        if not data.get("total_assets"):
            issues.append("Missing total_assets")
            score -= 25

        if not data.get("net_profit"):
            issues.append("Missing net_profit")
            score -= 25

        if not data.get("total_operating_income"):
            issues.append("Missing revenue")
            score -= 20

        # logical checks
        if data.get("total_assets") and data.get("total_equity"):
            if data["total_equity"] > data["total_assets"]:
                issues.append("Equity > Assets (invalid)")
                score -= 20

        # negative checks
        for k, v in data.items():
            if isinstance(v, (int, float)) and v < 0:
                if k not in ["credit_loss"]:
                    issues.append(f"Negative value in {k}")
                    score -= 10

        return issues, max(score, 0)

    # -------------------------------
    def detect_period(self, text):
        text = text.lower()

        if "for the year ended" in text or "31 december" in text:
            return "annual"
        if "nine months" in text:
            return "9M"
        if "six months" in text:
            return "H1"
        if "three months" in text:
            return "Q1"

        return "unknown"

    # -------------------------------
    def extract_tables(self, pdf_path):
        tables = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                for t in page.extract_tables():
                    tables.append(pd.DataFrame(t))
        return tables

    # -------------------------------
    def extract_all(self, pdf_path):

        tables = self.extract_tables(pdf_path)

        text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for p in pdf.pages:
                t = p.extract_text()
                if t:
                    text += t.lower()

        d = {}

        for df in tables:
            txt = " ".join(df.astype(str).values.flatten()).lower()

            for i in range(len(df)):
                row = " ".join(df.iloc[i].astype(str)).lower()

                if "total assets" in row:
                    d["total_assets"] = self.clean_value(df.iloc[i,1])
                elif "total liabilities" in row:
                    d["total_liabilities"] = self.clean_value(df.iloc[i,1])
                elif "total equity" in row:
                    d["total_equity"] = self.clean_value(df.iloc[i,1])
                elif "interest income" in row:
                    d["interest_income"] = self.clean_value(df.iloc[i,1])
                elif "net interest income" in row:
                    d["net_interest_income"] = self.clean_value(df.iloc[i,1])
                elif "total operating income" in row:
                    d["total_operating_income"] = self.clean_value(df.iloc[i,1])
                elif "expenses" in row:
                    d["operating_expenses"] = self.clean_value(df.iloc[i,1])
                elif "credit loss" in row:
                    d["credit_loss"] = self.clean_value(df.iloc[i,1])
                elif "profit" in row:
                    d["net_profit"] = self.clean_value(df.iloc[i,1])
                elif "loans" in row:
                    d["loans"] = self.clean_value(df.iloc[i,1])
                elif "deposits" in row:
                    d["deposits"] = self.clean_value(df.iloc[i,1])

        # ratios
        if d.get("net_profit") and d.get("total_equity"):
            d["roe"] = round((d["net_profit"]/d["total_equity"])*100,2)

        if d.get("loans") and d.get("deposits"):
            d["loan_to_deposit"] = round(d["loans"]/d["deposits"],2)

        if d.get("operating_expenses") and d.get("total_operating_income"):
            d["cost_to_income"] = round(d["operating_expenses"]/d["total_operating_income"],2)

        return d, text

    # -------------------------------
    def merge_year(self, pdata):
        priority = ["annual", "9M", "H1", "Q1"]

        final = {}
        for p in priority:
            if p in pdata:
                for k, v in pdata[p].items():
                    if v and k not in final:
                        final[k] = v
        return final

    # -------------------------------
    def run(self):

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        banks = cursor.execute("SELECT bank_name FROM banks").fetchall()

        for (bank,) in banks:

            base = os.path.join(BASE_PATH, bank, "financial_report")
            if not os.path.exists(base):
                continue

            for year_folder in os.listdir(base):

                if not year_folder.isdigit():
                    continue

                year = int(year_folder)
                year_path = os.path.join(base, year_folder)

                pdata = {}

                for file in os.listdir(year_path):

                    if not file.endswith(".pdf"):
                        continue

                    file_path = os.path.join(year_path, file)

                    print(f"📄 {bank} | {year} | {file}")

                    data, text = self.extract_all(file_path)

                    period = self.detect_period(text)

                    if period == "unknown":
                        self.log_issue(cursor, bank, year, f"Unknown period: {file}")
                        continue

                    pdata[period] = data

                final = self.merge_year(pdata)

                issues, score = self.validate_data(final)

                print(f"💾 FINAL ({score}% confidence) → {final}")

                # log issues
                for issue in issues:
                    self.log_issue(cursor, bank, year, issue)

                # skip very bad data
                if score < 40:
                    print("❌ Skipping due to low confidence")
                    continue

                cursor.execute("""
                INSERT OR REPLACE INTO financial_full (
                    bank_name, year, period_type,
                    interest_income, net_interest_income, fee_income,
                    total_operating_income, operating_expenses, credit_loss, net_profit,
                    total_assets, total_liabilities, total_equity, loans, deposits,
                    operating_cashflow, investing_cashflow, financing_cashflow,
                    roe, loan_to_deposit, cost_to_income, car, tier1_ratio, cet1_ratio
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    bank, year, "final",
                    final.get("interest_income"),
                    final.get("net_interest_income"),
                    final.get("fee_income"),
                    final.get("total_operating_income"),
                    final.get("operating_expenses"),
                    final.get("credit_loss"),
                    final.get("net_profit"),
                    final.get("total_assets"),
                    final.get("total_liabilities"),
                    final.get("total_equity"),
                    final.get("loans"),
                    final.get("deposits"),
                    final.get("operating_cashflow"),
                    final.get("investing_cashflow"),
                    final.get("financing_cashflow"),
                    final.get("roe"),
                    final.get("loan_to_deposit"),
                    final.get("cost_to_income"),
                    final.get("car"),
                    final.get("tier1_ratio"),
                    final.get("cet1_ratio")
                ))

        conn.commit()
        conn.close()

        print("\n✅ DONE WITH VALIDATION\n")


def main():
    FinancialExtractor().run()