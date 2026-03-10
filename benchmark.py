import os
import pandas as pd
from scripts.processor import TextProcessor
from scripts.root_cause_analyzer import RootCauseAnalyzer

# =====================================================
# CONFIG
# =====================================================
# =====================================================
# AUTO DISCOVER BANKS
# =====================================================

def discover_banks(base_path):

    banks = {}

    for bank_folder in os.listdir(base_path):

        bank_path = os.path.join(base_path, bank_folder)

        if not os.path.isdir(bank_path):
            continue

        reviews_path = os.path.join(bank_path, "Reviews")

        if os.path.exists(reviews_path):

            # Convert folder name → readable bank name
            bank_name = bank_folder.replace("_", " ")

            banks[bank_name] = reviews_path

    return banks

OUTPUT_DIR = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output"
BENCHMARK_OUTPUT = os.path.join(OUTPUT_DIR, "bank_benchmark_report.txt")
BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"

# =====================================================
# LOAD REVIEWS (MULTI-SHEET SUPPORT)
# =====================================================

def load_texts_from_folder(folder_path):

    texts = []

    for file in os.listdir(folder_path):

        if not file.endswith(".xlsx"):
            continue

        full_path = os.path.join(folder_path, file)

        print(f"\n📄 Loading file: {file}")

        try:
            xls = pd.ExcelFile(full_path)
        except Exception as e:
            print("⚠ Cannot open file:", e)
            continue

        # Read ALL sheets
        for sheet in xls.sheet_names:

            try:
                df = pd.read_excel(xls, sheet_name=sheet)
            except:
                continue

            print(f"   → Sheet: {sheet}")

            if "review" not in df.columns:
                print("   ⚠ 'review' column not found. Skipping.")
                continue

            reviews = df["review"].dropna().astype(str)

            reviews = reviews[reviews.str.strip() != ""]

            texts.extend(reviews.tolist())

    return texts


# =====================================================
# RISK LABEL
# =====================================================

def risk_label(score):

    if score < -0.6:
        return "Critical Risk"
    elif score < -0.4:
        return "High Risk"
    elif score < -0.2:
        return "Elevated Risk"
    elif score < 0:
        return "Moderate Risk"
    else:
        return "Positive"


# =====================================================
# MAIN EXECUTION
# =====================================================

def main():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    processor = TextProcessor()
    rca = RootCauseAnalyzer()

    benchmark_results = []

    print("\n🏦 Running Cross-Bank Benchmark...\n")

    banks = discover_banks(BASE_CORP_PATH)

    for bank, path in banks.items():

        if not os.path.exists(path):
            print(f"⚠ Folder not found for {bank}")
            continue

        texts = load_texts_from_folder(path)

        print(f"\n--- DEBUG: {bank} ---")
        print("Number of texts:", len(texts))
        print("Sample reviews:", texts[:3])
        print("----------------------\n")

        if len(texts) == 0:
            print(f"⚠ No reviews found for {bank}")
            continue

        # =========================
        # PROCESS TEXT
        # =========================
        sentiments, clusters, metrics = processor.process(texts)

        # =========================
        # ROOT CAUSE ANALYSIS
        # =========================
        print(f"🔎 Running Root Cause Analysis for {bank}...\n")

        rca.analyze(
            texts=texts,
            sentiments=sentiments,
            bank_name=bank,
            save_to_file=True,
            output_dir=OUTPUT_DIR,
            verbose=True
        )

        # =========================
        # STORE BENCHMARK RESULT
        # =========================
        benchmark_results.append({
            "bank": bank,
            "total_reviews": metrics["total_reviews"],
            "overall_sentiment": metrics["overall_sentiment"],
            "risk": risk_label(metrics["overall_sentiment"])
        })

    # =========================
    # SORT BY WORST SENTIMENT
    # =========================
    benchmark_results.sort(key=lambda x: x["overall_sentiment"])

    # =========================
    # BUILD BENCHMARK REPORT
    # =========================
    lines = []
    lines.append("THAI BANK MOBILE SENTIMENT BENCHMARK")
    lines.append("====================================\n")

    for i, bank_data in enumerate(benchmark_results, 1):

        lines.append(
            f"{i}. {bank_data['bank']} → "
            f"{bank_data['overall_sentiment']:.3f} "
            f"({bank_data['risk']})"
        )

    report_text = "\n".join(lines)

    with open(BENCHMARK_OUTPUT, "w", encoding="utf-8") as f:
        f.write(report_text)

    print("\n📊 BENCHMARK RESULTS\n")
    print(report_text)
    print("\n📄 Benchmark saved to:", BENCHMARK_OUTPUT)


# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":
    main()