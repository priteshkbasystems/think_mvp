import os
import sqlite3
import pandas as pd

from scripts.db_cache import init_db, register_bank, get_bank_id

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


# ---------------------------------------
# Save yearly stock return
# ---------------------------------------

def save_stock_return(bank_name, year, value):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("INSERT OR IGNORE INTO banks (bank_name) VALUES (?)", (bank_name,))
    cursor.execute("SELECT bank_id FROM banks WHERE bank_name=?", (bank_name,))
    bank_id = cursor.fetchone()[0]
    cursor.execute("""
    INSERT OR REPLACE INTO stock_returns
    (bank_id, bank_name, year, return)
    VALUES (?, ?, ?, ?)
    """, (bank_id, bank_name, year, value))

    conn.commit()
    conn.close()


# ---------------------------------------
# Compute yearly returns
# ---------------------------------------

def compute_yearly_returns(file_path):

    try:

        # Load file
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        # Required columns check
        if "Date" not in df.columns or "Price" not in df.columns:
            print("⚠ Missing Date or Price column:", file_path)
            return {}

        # Parse date safely
        df["Date"] = pd.to_datetime(
            df["Date"],
            errors="coerce",
            dayfirst=True
        )

        # Convert price safely
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

        # Remove invalid rows
        df = df.dropna(subset=["Date", "Price"])

        if len(df) == 0:
            return {}

        # Extract year
        df["year"] = df["Date"].dt.year

        # Sort by date
        df = df.sort_values("Date")

        results = {}

        # Compute yearly returns
        for year, g in df.groupby("year"):

            if len(g) < 2:
                continue

            start = g.iloc[0]["Price"]
            end = g.iloc[-1]["Price"]

            if start == 0:
                continue

            ret = (end - start) / start

            results[int(year)] = float(ret)

        return results

    except Exception as e:

        print("❌ Stock return error:", file_path, e)

        return {}


# ---------------------------------------
# Discover banks
# ---------------------------------------

def discover_banks(base_path):

    banks = {}

    if not os.path.exists(base_path):
        print("⚠ Base path not found:", base_path)
        return banks

    for name in os.listdir(base_path):

        path = os.path.join(base_path, name)

        if not os.path.isdir(path):
            continue

        stock_file = None

        stock_folder = os.path.join(path, "stock_price")

        if os.path.exists(stock_folder):

            for f in os.listdir(stock_folder):

                if f.endswith(".xlsx") or f.endswith(".csv"):
                    stock_file = os.path.join(stock_folder, f)

        banks[name] = {
            "folder": path,
            "stock": stock_file
        }

    return banks


# ---------------------------------------
# Index stock data
# ---------------------------------------

def index_stock_data(bank_name, stock_file):

    if not stock_file:
        return

    returns = compute_yearly_returns(stock_file)

    for year, value in returns.items():

        save_stock_return(bank_name, year, value)


# ---------------------------------------
# Main
# ---------------------------------------

def main():

    print("🔎 Running Data Indexer...")

    # Ensure DB tables exist
    init_db()

    banks = discover_banks(BASE_CORP_PATH)

    if not banks:
        print("⚠ No banks discovered.")
        return

    for bank_name, info in banks.items():

        print(f"📊 Indexing {bank_name}")

        register_bank(bank_name)

        if info["stock"]:
            index_stock_data(bank_name, info["stock"])

    print("✔ Data indexing completed")