import os
import re
import sqlite3
import pandas as pd
import sys
import pdfplumber

sys.path.insert(0, "/content/drive/MyDrive/THINK_MVP")

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

print("🔥 FINAL FINANCIAL EXTRACTOR (ENTERPRISE AI ENGINE) LOADED 🔥")


class FinancialExtractor:

    def __init__(self):
        print("\n🚀 FULL FINANCIAL ENGINE (MULTI-PDF + MULTI-YEAR)\n")

    # -------------------------------
    # CLEAN VALUE
    # -------------------------------
    def clean_value(self, v):
        try:
            v = str(v).replace(",", "").replace("%", "").strip()
            return float(v)
        except:
            return None

    # -------------------------------
    # NORMALIZE
    # -------------------------------
    def normalize_name(self, name):
        return name.lower().replace(" ", "").replace("_", "")

    def find_bank_folder(self, bank_name):
        for folder in os.listdir(BASE_PATH):
            if self.normalize_name(folder) == self.normalize_name(bank_name):
                return os.path.join(BASE_PATH, folder)
        return None

    # -------------------------------
    # PERIOD DETECTION
    # -------------------------------
    def detect_period(self, file_name):
        f = file_name.lower()

        if "march" in f:
            return "Q1"
        elif "june" in f:
            return "H1"
        elif "september" in f:
            return "9M"
        elif "december" in f:
            return "annual"

        return "unknown"

    # -------------------------------
    # PDF TABLE EXTRACTION
    # -------------------------------
    def extract_tables(self, pdf_path):
        tables = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_tables()
                for tb in t:
                    tables.append(pd.DataFrame(tb))
        return tables

    # -------------------------------
    # BALANCE SHEET
    # -------------------------------
    def extract_balance(self, tables):
        d = {}

        for df in tables:
            txt = " ".join(df.astype(str).values.flatten()).lower()

            if "total assets" in txt:

                for i in range(len(df)):
                    row = " ".join(df.iloc[i].astype(str)).lower()

                    if "total assets" in row:
                        d["total_assets"] = self.clean_value(df.iloc[i,1])
                    elif "total liabilities" in row:
                        d["total_liabilities"] = self.clean_value(df.iloc[i,1])
                    elif "total equity" in row:
                        d["total_equity"] = self.clean_value(df.iloc[i,1])
                    elif "loans" in row:
                        d["loans"] = self.clean_value(df.iloc[i,1])
                    elif "deposits" in row:
                        d["deposits"] = self.clean_value(df.iloc[i,1])

        return d

    # -------------------------------
    # INCOME STATEMENT
    # -------------------------------
    def extract_income(self, tables):
        d = {}

        for df in tables:
            txt = " ".join(df.astype(str).values.flatten()).lower()

            if "interest income" in txt:

                for i in range(len(df)):
                    row = " ".join(df.iloc[i].astype(str)).lower()

                    if "interest income" in row:
                        d["interest_income"] = self.clean_value(df.iloc[i,1])
                    elif "net interest income" in row:
                        d["net_interest_income"] = self.clean_value(df.iloc[i,1])
                    elif "fees" in row:
                        d["fee_income"] = self.clean_value(df.iloc[i,1])
                    elif "total operating income" in row:
                        d["total_operating_income"] = self.clean_value(df.iloc[i,1])
                    elif "expenses" in row:
                        d["operating_expenses"] = self.clean_value(df.iloc[i,1])
                    elif "credit loss" in row:
                        d["credit_loss"] = self.clean_value(df.iloc[i,1])
                    elif "profit" in row:
                        d["net_profit"] = self.clean_value(df.iloc[i,1])

        return d

    # -------------------------------
    # CASH FLOW
    # -------------------------------
    def extract_cashflow(self, tables):
        d = {}

        for df in tables:
            txt = " ".join(df.astype(str).values.flatten()).lower()

            if "cash flows" in txt:

                for i in range(len(df)):
                    row = " ".join(df.iloc[i].astype(str)).lower()

                    if "operating activities" in row:
                        d["operating_cashflow"] = self.clean_value(df.iloc[i,1])
                    elif "investing activities" in row:
                        d["investing_cashflow"] = self.clean_value(df.iloc[i,1])
                    elif "financing activities" in row:
                        d["financing_cashflow"] = self.clean_value(df.iloc[i,1])

        return d

    # -------------------------------
    # RATIOS (TEXT)
    # -------------------------------
    def extract_ratios(self, pdf_path):
        text = ""

        with pdfplumber.open(pdf_path) as pdf:
            for p in pdf.pages:
                t = p.extract_text()
                if t:
                    text += t.lower()

        d = {}

        patterns = {
            "car": r"capital adequacy ratio[^0-9]+([\d\.]+)",
            "tier1_ratio": r"tier 1[^0-9]+([\d\.]+)",
            "cet1_ratio": r"common equity tier 1[^0-9]+([\d\.]+)"
        }

        for k, p in patterns.items():
            m = re.search(p, text)
            if m:
                d[k] = float(m.group(1))

        return d

    # -------------------------------
    # COMPUTE RATIOS
    # -------------------------------
    def compute_ratios(self, d):

        if d.get("net_profit") and d.get("total_equity"):
            d["roe"] = round((d["net_profit"]/d["total_equity"])*100,2)

        if d.get("loans") and d.get("deposits"):
            d["loan_to_deposit"] = round(d["loans"]/d["deposits"],2)

        if d.get("operating_expenses") and d.get("total_operating_income"):
            d["cost_to_income"] = round(d["operating_expenses"]/d["total_operating_income"],2)

        return d

    # -------------------------------
    # PROCESS PDF
    # -------------------------------
    def process_pdf(self, pdf_path):

        tables = self.extract_tables(pdf_path)

        data = {}
        data.update(self.extract_balance(tables))
        data.update(self.extract_income(tables))
        data.update(self.extract_cashflow(tables))
        data.update(self.extract_ratios(pdf_path))

        return self.compute_ratios(data)

    # -------------------------------
    # MERGE PERIOD DATA
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
    # RUN
    # -------------------------------
    def run(self):

        print("\n🚀 STARTING FULL EXTRACTION\n")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT bank_name FROM banks")
        banks = cursor.fetchall()

        for (bank,) in banks:

            print(f"\n🏦 {bank}")

            bank_folder = self.find_bank_folder(bank)
            if not bank_folder:
                continue

            base = os.path.join(bank_folder, "financial_report")

            if not os.path.exists(base):
                continue

            for year_folder in os.listdir(base):

                year_path = os.path.join(base, year_folder)

                if not os.path.isdir(year_path) or not year_folder.isdigit():
                    continue

                year = int(year_folder)
                print(f"\n📅 {year}")

                pdata = {}

                for file in os.listdir(year_path):

                    if not file.endswith(".pdf"):
                        continue

                    period = self.detect_period(file)
                    file_path = os.path.join(year_path, file)

                    print(f"   📄 {file} → {period}")

                    pdata[period] = self.process_pdf(file_path)

                final = self.merge_year(pdata)
                final = self.compute_ratios(final)

                print(f"💾 FINAL → {final}")

                cursor.execute("""
                INSERT OR REPLACE INTO financial_full VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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

        print("\n✅ DONE\n")


def main():
    FinancialExtractor().run()