import os
import pandas as pd
from scripts.processor import TextProcessor
from scripts.root_cause_analyzer import RootCauseAnalyzer

BANK_PATHS = {
    "Krungthai Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/Krungthai_Bank/Reviews",
    "Kasikornbank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/KBank/Reviews",
    "SCB_Pre2022 Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/SCB_Pre2022/Reviews",
    "SCB X": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/SCBX_CardX/Reviews",
}

OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_benchmark_report.txt"


def load_texts_from_folder(folder_path):
    texts = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            df = pd.read_excel(os.path.join(folder_path, file))
            texts.extend(df["review"].dropna().astype(str).tolist())

    return texts


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


def main():
    processor = TextProcessor()
    rca = RootCauseAnalyzer()
    benchmark_results = []

    print("\n🏦 Running Cross-Bank Benchmark...\n")

    for bank, path in BANK_PATHS.items():

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
        root_causes = rca.analyze(texts, sentiments)

        total_negative = sum(root_causes.values())

        print("🔍 ROOT CAUSE BREAKDOWN:")

        for cause, count in root_causes.most_common():
            percentage = (count / total_negative) * 100 if total_negative > 0 else 0
            print(f"{cause}: {count} ({percentage:.1f}%)")

        print("\n")

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
    # BUILD REPORT
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

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)

    print("\n📊 BENCHMARK RESULTS\n")
    print(report_text)
    print("\n📄 Saved to:", OUTPUT_PATH)


if __name__ == "__main__":
    main()