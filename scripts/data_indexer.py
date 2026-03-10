import os
import pandas as pd

from scripts.db_cache import (
    init_db,
    register_bank,
    save_stock_return
)

# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


# ==========================================
# DISCOVER BANKS
# ==========================================

def discover_banks(base_path):

    banks = {}

    if not os.path.exists(base_path):
        print("❌ Base path not found:", base_path)
        return banks

    for bank_folder in sorted(os.listdir(base_path)):

        bank_path = os.path.join(base_path, bank_folder)

        if not os.path.isdir(bank_path):
            continue

        display_name = bank_folder.replace("_", " ")

        stock_folder = os.path.join(bank_path, "stock_price")

        stock_file = None

        if os.path.exists(stock_folder):

            for file in os.listdir(stock_folder):

                if file.endswith(".xlsx") or file.endswith(".csv"):
                    stock_file = os.path.join(stock_folder, file)

        banks[display_name] = {
            "folder": bank_path,
            "stock": stock_file
        }

    return banks


# ==========================================
# LOAD STOCK FILE (CSV OR XLSX)
# ==========================================

def load_stock_dataframe(file_path):

    if file_path.endswith(".xlsx"):

        xls = pd.ExcelFile(file_path)

        for sheet in xls.sheet_names:

            df = pd.read_excel(xls, sheet_name=sheet)

            if "Date" in df.columns and "Price" in df.columns:
                return df

        return None

    else:

        try:
            return pd.read_csv(file_path, encoding="utf-8")
        except:
            return pd.read_csv(file_path, encoding="latin1")


# ==========================================
# COMPUTE YEARLY RETURNS
# ==========================================

def compute_yearly_returns(file_path):

    df = load_stock_dataframe(file_path)

    if df is None:
        print("⚠ No valid sheet with Date/Price found.")
        return {}

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Date"])

    df["Price"] = (
        df["Price"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    df["Year"] = df["Date"].dt.year

    yearly_returns = {}

    for year in sorted(df["Year"].unique()):

        year_df = df[df["Year"] == year].sort_values("Date")

        if len(year_df) < 2:
            continue

        first_price = year_df.iloc[0]["Price"]
        last_price = year_df.iloc[-1]["Price"]

        yearly_return = (last_price - first_price) / first_price

        yearly_returns[int(year)] = yearly_return

    return yearly_returns


# ==========================================
# INDEX STOCK DATA INTO SQLITE
# ==========================================

def index_stock_data(bank_name, stock_file):

    if not stock_file:
        print(f"⚠ No stock data for {bank_name}")
        return

    print(f"📊 Processing stock data for {bank_name}")

    returns = compute_yearly_returns(stock_file)

    for year, value in returns.items():

        save_stock_return(bank_name, year, value)

        print(f"   {year} → {value:.3f}")


# ==========================================
# MAIN
# ==========================================

def main():

    print("\n🔎 Running Data Indexer...\n")

    init_db()

    banks = discover_banks(BASE_CORP_PATH)

    print(f"Detected {len(banks)} banks\n")

    for bank, components in banks.items():

        print(f"🏦 Registering bank: {bank}")

        register_bank(bank)

        index_stock_data(bank, components["stock"])

    print("\n✅ Data indexing complete.\n")


if __name__ == "__main__":
    main()