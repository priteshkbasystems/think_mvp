import os
import pandas as pd
from scripts.processor import TextProcessor


BANK_PATHS = {
    "Krungthai Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/Krungthai_Bank/Reviews",
    "Kasikornbank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/KBank/Reviews",
    "Bangkok Bank": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/SCB_Pre2022/Reviews",
    "SCB X": "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/SCBX_CardX/Reviews",
}

OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_benchmark_report.txt"


def load_texts_from_folder(folder_path):
    texts = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            df = pd.read_excel(os.path.join(folder_path, file))
            first_column = df.columns[0]
            texts.extend(df[first_column].dropna().astype(str).tolist())

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
    benchmark_results = []

    print("\n🏦 Running Cross-Bank Benchmark...\n")

    for bank, path in BANK_PATHS.items():
        if not os.path.exists(path):
            print(f"⚠ Folder not found for {bank}")
            continue

        texts = load_texts_from_folder(path)

        if len(texts) == 0:
            print(f"⚠ No reviews found for {bank}")
            continue

        _, _, metrics = processor.process(texts)

        benchmark_results.append({
            "bank": bank,
            "total_reviews": metrics["total_reviews"],
            "overall_sentiment": metrics["overall_sentiment"],
            "risk": risk_label(metrics["overall_sentiment"])
        })

    # Sort by worst sentiment
    benchmark_results.sort(key=lambda x: x["overall_sentiment"])

    # Build Report
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