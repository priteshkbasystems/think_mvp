import os
import re
import json
import numpy as np
from PyPDF2 import PdfReader
from scipy.stats import pearsonr
from trend_analysis import main as run_trend_engine

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

import pytesseract
from pdf2image import convert_from_path

from scripts.db_cache import (
    init_db,
    get_file_modified_time,
    get_cached_score,
    update_cache,
    get_embedding,
    save_embedding
)

init_db()

# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
TREND_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output"
FINAL_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_correlation_report.txt"

MAX_SENTENCES = 300

TRANSFORMATION_THEMES = [

    # Digital Transformation Core
    "digital transformation",
    "digital banking",
    "digital strategy",
    "digital operating model",
    "digital innovation",

    # Customer Experience Transformation
    "digital customer experience",
    "customer experience",
    "customer journey",
    "omnichannel banking",
    "mobile banking",
    "personalized banking",

    # Artificial Intelligence & Data
    "artificial intelligence",
    "AI",
    "machine learning",
    "advanced analytics",
    "data analytics",
    "predictive analytics",
    "data driven",
    "big data",
    "data platform",

    # Automation & Efficiency
    "automation",
    "robotic process automation",
    "RPA",
    "intelligent automation",
    "process automation",
    "operational efficiency",

    # Digital Banking Platforms
    "mobile banking platform",
    "online banking",
    "digital platform",
    "next generation banking platform",
    "platform banking",

    # Payments Innovation
    "digital payments",
    "real time payments",
    "cashless society",
    "contactless payment",
    "QR payment",
    "mobile wallet",

    # Fintech & Ecosystem
    "fintech",
    "fintech partnership",
    "open banking",
    "banking ecosystem",
    "platform ecosystem",
    "banking as a service",
    "API banking",
    "developer platform",

    # Cloud & Infrastructure
    "cloud computing",
    "cloud infrastructure",
    "cloud native",
    "microservices",
    "core banking modernization",
    "IT modernization",

    # Cybersecurity & Digital Risk
    "cybersecurity",
    "digital security",
    "digital identity",
    "fraud detection",
    "authentication platform",

    # Innovation & R&D
    "innovation lab",
    "innovation center",
    "research and development",
    "technology innovation",
    "digital product innovation",

    # ESG & Sustainable Transformation
    "sustainability",
    "ESG",
    "green finance",
    "sustainable finance",
    "climate finance",

    # Business Model Transformation
    "business model transformation",
    "digital first strategy",
    "branch to digital",
    "future of banking",
    "banking transformation"
]


# ==========================================
# LOAD AI MODEL
# ==========================================

print("Loading AI transformation model...")

embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

THEME_EMBEDDINGS = embedding_model.encode(TRANSFORMATION_THEMES)

print("Model ready.")


# ==========================================
# OCR EXTRACTION
# ==========================================

def extract_text_with_ocr(pdf_path):

    text = ""

    try:

        images = convert_from_path(pdf_path, dpi=200)

        for img in images:
            text += pytesseract.image_to_string(img)

    except Exception:
        print("OCR failed for:", pdf_path)

    return text.lower()


# ==========================================
# PDF TEXT EXTRACTION
# ==========================================

def extract_text_from_pdf(pdf_path):

    try:

        reader = PdfReader(pdf_path)

        text = ""

        for page in reader.pages:
            text += page.extract_text() or ""

        text = text.lower()

        # If almost no text extracted → run OCR
        if len(text.strip()) < 100:

            print("Running OCR for:", os.path.basename(pdf_path))

            text = extract_text_with_ocr(pdf_path)

        return text

    except Exception:

        print("PDF read failed:", pdf_path)

        return ""


# ==========================================
# AUTO BANK DISCOVERY
# ==========================================

def discover_banks(base_path):

    banks = {}

    for bank_folder in os.listdir(base_path):

        bank_path = os.path.join(base_path, bank_folder)

        if not os.path.isdir(bank_path):
            continue

        components = {
            "annual_reports": None,
            "investor_presentations": None
        }

        for sub in os.listdir(bank_path):

            sub_path = os.path.join(bank_path, sub)

            if not os.path.isdir(sub_path):
                continue

            sub_lower = sub.lower()

            if sub_lower == "annual_reports":
                components["annual_reports"] = sub_path

            elif sub_lower in ["investor_presentations", "investors_presentations"]:
                components["investor_presentations"] = sub_path

        banks[bank_folder] = components

    return banks


# ==========================================
# EXTRACT YEAR
# ==========================================

def extract_year_from_path(path):

    match = re.search(r"(20\d{2})", path)

    if match:
        return int(match.group(1))

    return None


# ==========================================
# TRANSFORMATION SCORE
# ==========================================

def compute_transformation_score(text):

    sentences = re.split(r"[.!?]", text)

    sentences = [s.strip() for s in sentences if len(s.strip()) > 30]

    sentences = sentences[:MAX_SENTENCES]

    if not sentences:
        return 0

    sentence_embeddings = []

    for sentence in sentences:

        emb = get_embedding(sentence)

        if emb is None:

            emb = embedding_model.encode([sentence])[0]

            save_embedding(sentence, emb)

        sentence_embeddings.append(emb)

    sentence_embeddings = np.array(sentence_embeddings)

    similarity_matrix = cosine_similarity(
        sentence_embeddings,
        THEME_EMBEDDINGS
    )

    max_scores = similarity_matrix.max(axis=1)

    threshold = 0.45

    # Count sentences that strongly match transformation themes
    strong_matches = [s for s in max_scores if s > threshold]

    total_sentences = len(max_scores)

    if total_sentences == 0:
        return 0.0

    # score = proportion of transformation sentences
    score = len(strong_matches) / total_sentences

    return score


# ==========================================
# SCAN PDF FOLDER
# ==========================================

def compute_scores_from_folder(folder_path):

    scores = {}

    if not folder_path or not os.path.exists(folder_path):
        return scores

    for root, dirs, files in os.walk(folder_path):

        for file in files:

            if not file.endswith(".pdf"):
                continue

            full_path = os.path.join(root, file)

            print("Processing PDF:", file)

            year = extract_year_from_path(root)

            if not year:
                continue

            last_modified = get_file_modified_time(full_path)

            cached = get_cached_score(full_path)

            if cached and cached[0] == last_modified:
                scores[cached[1]] = cached[2]
                continue

            text = extract_text_from_pdf(full_path)

            if not text:
                continue

            intensity_score = compute_transformation_score(text)

            scores[year] = intensity_score

            update_cache(full_path, last_modified, year, intensity_score)

    return scores


# ==========================================
# NORMALIZE
# ==========================================

def normalize_scores(scores):

    if not scores:
        return scores

    max_val = max(scores.values())

    if max_val > 0:

        for year in scores:
            scores[year] = scores[year] / max_val

    return scores


# ==========================================
# LOAD SENTIMENT
# ==========================================

def load_sentiment_trend():

    trend_file = os.path.join(
        TREND_OUTPUT_PATH,
        "bank_trend_data.json"
    )

    if not os.path.exists(trend_file):

        print("⚠ Sentiment trend data not found. Running Trend Engine...")

        run_trend_engine()

    if not os.path.exists(trend_file):

        print("❌ Trend generation failed.")

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

    banks = discover_banks(BASE_CORP_PATH)

    sentiment_trends = load_sentiment_trend()

    report_lines = []

    report_lines.append("TRANSFORMATION IMPACT CORRELATION REPORT")
    report_lines.append("=========================================\n")

    for bank_folder, components in banks.items():

        display_name = bank_folder.replace("_", " ")

        print(f"\nAnalyzing {display_name}...")

        annual_scores = compute_scores_from_folder(components["annual_reports"])

        investor_scores = compute_scores_from_folder(components["investor_presentations"])

        transformation_scores = annual_scores.copy()

        for year, score in investor_scores.items():

            if year in transformation_scores:

                transformation_scores[year] = (
                    transformation_scores[year] + score
                ) / 2

            else:
                transformation_scores[year] = score

        transformation_scores = normalize_scores(transformation_scores)

        sentiment_scores = sentiment_trends.get(display_name, {})

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
            impact = "Negative Impact"

        report_lines.append(f"Impact Assessment: {impact}")

    final_report = "\n".join(report_lines)

    with open(FINAL_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(final_report)

    print("\n📄 Report saved to:", FINAL_OUTPUT_PATH)

    print("\n" + final_report)


if __name__ == "__main__":
    main()