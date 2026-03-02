import os
import re
import json
import numpy as np
from PyPDF2 import PdfReader
from scipy.stats import pearsonr

# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
TREND_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output"
FINAL_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_correlation_report.txt"

# 🔥 Display Name → Folder Name mapping
BANK_CONFIG = {
    "Krungthai Bank": "Krungthai_Bank",
    "Kasikornbank": "KBank",
    "SCB_Pre2022 Bank": "SCB_Pre2022"
}

# Transformation keywords
TRANSFORMATION_KEYWORDS = [
    "digital",
    "mobile",
    "platform",
    "ecosystem",
    "ai",
    "artificial intelligence",
    "machine learning",
    "automation",
    "analytics",
    "customer experience",
    "innovation",
    "technology",
    "upgrade"
]

# ==========================================
# PDF TEXT EXTRACTION
# ==========================================

def extract_text_from_pdf(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text.lower()
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return ""

# ==========================================
# YEAR EXTRACTION
# ==========================================

def extract_year_from_filename(filename):
    match = re.search(r"(20\d{2})", filename)
    return int(match.group(1)) if match else None

# ==========================================
# TRANSFORMATION INTENSITY
# ==========================================

def compute_transformation_scores(bank_path):

    annual_path = os.path.join(bank_path, "Annual_Reports")
    scores = {}

    if not os.path.exists(annual_path):
        print(f"⚠ Annual_Reports folder not found: {annual_path}")
        return scores

    for file in os.listdir(annual_path):

        if not file.endswith(".pdf"):
            continue

        year = extract_year_from_filename(file)
        if not year:
            continue

        full_path = os.path.join(annual_path, file)
        text = extract_text_from_pdf(full_path)

        if not text:
            continue

        words = re.findall(r"\b\w+\b", text)
        total_words = len(words)

        if total_words == 0:
            continue

        keyword_count = 0

        for keyword in TRANSFORMATION_KEYWORDS:
            pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
            keyword_count += len(re.findall(pattern, text))

        intensity_score = keyword_count / total_words
        scores[year] = intensity_score

    # Normalize per bank
    if scores:
        max_val = max(scores.values())
        if max_val > 0:
            for year in scores:
                scores[year] = scores[year] / max_val

    return scores

# ==========================================
# LOAD SENTIMENT TREND (JSON)
# ==========================================

def load_sentiment_trend():

    trend_file = os.path.join(
        TREND_OUTPUT_PATH,
        "bank_trend_data.json"
    )

    if not os.path.exists(trend_file):
        print("⚠ Trend JSON not found.")
        return {}

    with open(trend_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    sentiment_data = {}

    for bank_name, bank_data in raw_data.items():
        sentiment_data[bank_name] = {
            int(year): score
            for year, score in bank_data["yearly_sentiment"].items()
        }

    return sentiment_data

# ==========================================
# CORRELATION
# ==========================================

def compute_correlation(transformation_scores, sentiment_scores):

    overlapping_years = sorted(
        set(transformation_scores.keys()) &
        set(year - 1 for year in sentiment_scores.keys())
    )

    if len(overlapping_years) < 2:
        return None

    x = []
    y = []

    for year in overlapping_years:
        next_year = year + 1
        if next_year in sentiment_scores:
            x.append(transformation_scores[year])
            y.append(sentiment_scores[next_year])

    if len(x) < 2:
        return None

    correlation, _ = pearsonr(x, y)
    return correlation

# ==========================================
# MAIN
# ==========================================

def main():

    print("\n🔎 Running Transformation Correlation Engine...\n")

    sentiment_trends = load_sentiment_trend()

    report_lines = []
    report_lines.append("TRANSFORMATION IMPACT CORRELATION REPORT")
    report_lines.append("=========================================\n")

    for display_name, folder_name in BANK_CONFIG.items():

        print(f"Analyzing {display_name}...")

        bank_path = os.path.join(BASE_CORP_PATH, folder_name)

        transformation_scores = compute_transformation_scores(bank_path)
        sentiment_scores = sentiment_trends.get(display_name, {})

        print("Transformation Years:", sorted(transformation_scores.keys()))
        print("Sentiment Years:", sorted(sentiment_scores.keys()))

        correlation = compute_correlation(transformation_scores, sentiment_scores)

        report_lines.append(f"\n🏦 {display_name}")

        if correlation is None:
            report_lines.append("Insufficient data for correlation.")
            continue

        report_lines.append(
            f"Correlation (Transformation → Next Year Sentiment): {correlation:.3f}"
        )

        if correlation > 0.7:
            impact = "High Positive Impact"
        elif correlation > 0.3:
            impact = "Moderate Positive Impact"
        elif correlation > -0.3:
            impact = "No Clear Impact"
        else:
            impact = "Negative Impact (Transformation not reflected in sentiment)"

        report_lines.append(f"Impact Assessment: {impact}")

    final_report = "\n".join(report_lines)

    with open(FINAL_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(final_report)

    print("\n📄 Report saved to:", FINAL_OUTPUT_PATH)
    print("\n" + final_report)


if __name__ == "__main__":
    main()