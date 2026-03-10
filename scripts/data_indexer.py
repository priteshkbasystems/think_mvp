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

                if file.endswith(".xlsx"):
                    stock_file = os.path.join(stock_folder, file)

        banks[display_name] = {
            "folder": bank_path,
            "stock": stock_file
        }

    return banks


# ==========================================
# COMPUTE STOCK RETURNS
# ==========================================

def compute_yearly_returns(csv_path):

    df = pd.read_csv(csv_path)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
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