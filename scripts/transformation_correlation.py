import os
import re
import json
import numpy as np
from PyPDF2 import PdfReader
from scipy.stats import pearsonr

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
    save_embedding,
    get_cached_pdf_text,
    save_pdf_text
)

from trend_analysis import main as run_trend_engine

init_db()

# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
TREND_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output"
FINAL_OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_correlation_report.txt"

MAX_SENTENCES = 300

# ==========================================
# DISCOVER BANKS (FIXED)
# ==========================================

def discover_banks(base_path):

    banks = {}

    if not os.path.exists(base_path):
        print("❌ Base path not found:", base_path)
        return banks

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
# TRANSFORMATION THEMES
# ==========================================

TRANSFORMATION_THEMES = [
    "digital transformation","digital banking strategy","digital banking","digital operating model",
    "technology driven banking","digital capability","digital organization",
    "digital customer experience","customer journey","omnichannel banking","mobile banking",
    "personalized financial services","digital service channels",
    "artificial intelligence","AI","machine learning","advanced analytics","data analytics",
    "predictive analytics","data driven","data platform","big data",
    "automation","process automation","robotic process automation","RPA",
    "intelligent automation","operational efficiency","digitization of processes",
    "digital platform","mobile banking platform","online banking platform",
    "next generation banking","platform banking",
    "digital payments","real time payments","cashless society","contactless payment",
    "QR payment","mobile wallet","digital payment ecosystem",
    "fintech","fintech partnership","open banking","banking ecosystem","platform ecosystem",
    "banking as a service","API banking","digital ecosystem",
    "cloud","cloud computing","cloud infrastructure","core banking modernization",
    "IT modernization","microservices","technology infrastructure",
    "cybersecurity","digital security","digital identity","authentication","fraud detection",
    "innovation lab","innovation center","technology innovation","digital innovation",
    "research and development","venture investment","digital product innovation",
    "sustainability","ESG","green finance","sustainable finance","climate finance",
    "business model transformation","digital first strategy","banking transformation",
    "future of banking","branch to digital","technology driven bank"
]

# ==========================================
# LOAD MODEL
# ==========================================

print("Loading AI transformation model...")
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
THEME_EMBEDDINGS = embedding_model.encode(TRANSFORMATION_THEMES)
print("Model ready.")

# ==========================================
# OCR
# ==========================================

def extract_text_with_ocr(pdf_path):

    text = ""

    try:
        images = convert_from_path(pdf_path, dpi=200)

        for img in images:
            text += pytesseract.image_to_string(img)

    except:
        print("OCR failed:", pdf_path)

    return text.lower()


# ==========================================
# PDF TEXT EXTRACTION (CACHED)
# ==========================================

def extract_text_from_pdf(pdf_path):

    cached_text = get_cached_pdf_text(pdf_path)

    if cached_text:
        print("✔ Using cached text:", os.path.basename(pdf_path))
        return cached_text

    print("📄 Extracting text:", os.path.basename(pdf_path))

    try:
        reader = PdfReader(pdf_path)
        text = ""

        for page in reader.pages:
            text += page.extract_text() or ""

        text = text.lower()

        if len(text.strip()) < 100:
            print("⚠ OCR fallback:", os.path.basename(pdf_path))
            text = extract_text_with_ocr(pdf_path)

        save_pdf_text(pdf_path, text)

        return text

    except:
        print("❌ Failed to read PDF:", pdf_path)
        return ""


# ==========================================
# FILE CACHE CHECK
# ==========================================

def should_process_pdf(file_path):

    last_modified = get_file_modified_time(file_path)
    cached = get_cached_score(file_path)

    if cached is None:
        return True, last_modified

    if cached[0] != last_modified:
        return True, last_modified

    return False, last_modified


# ==========================================
# SCORE
# ==========================================

def compute_transformation_score(text):

    sentences = re.split(r"[.!?]", text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 30]
    sentences = sentences[:MAX_SENTENCES]

    if not sentences:
        return 0

    embeddings = []

    for s in sentences:

        emb = get_embedding(s)

        if emb is None:
            emb = embedding_model.encode([s])[0]
            save_embedding(s, emb)

        embeddings.append(emb)

    embeddings = np.array(embeddings)

    sim = cosine_similarity(embeddings, THEME_EMBEDDINGS)
    scores = sim.max(axis=1)

    strong = [x for x in scores if x > 0.45]

    return len(strong) / len(scores)


# ==========================================
# PROCESS FOLDER
# ==========================================

def compute_scores_from_folder(folder_path):

    scores = {}

    if not folder_path or not os.path.exists(folder_path):
        return scores

    for root, _, files in os.walk(folder_path):

        for file in files:

            if not file.endswith(".pdf"):
                continue

            path = os.path.join(root, file)

            year = extract_year_from_path(root)
            if not year:
                continue

            should_run, last_modified = should_process_pdf(path)

            if not should_run:
                print("✔ Skipping unchanged:", file)
                cached = get_cached_score(path)
                if cached:
                    scores[cached[1]] = cached[2]
                continue

            text = extract_text_from_pdf(path)

            if not text:
                continue

            score = compute_transformation_score(text)

            scores[year] = score

            update_cache(path, last_modified, year, score)

    return scores


# ==========================================
# UTILS
# ==========================================

def extract_year_from_path(path):
    match = re.search(r"(20\d{2})", path)
    return int(match.group(1)) if match else None


def normalize_scores(scores):

    if not scores:
        return scores

    max_val = max(scores.values())

    if max_val > 0:
        for y in scores:
            scores[y] /= max_val

    return scores


# ==========================================
# LOAD SENTIMENT
# ==========================================

def load_sentiment_trend():

    path = os.path.join(TREND_OUTPUT_PATH, "bank_trend_data.json")

    if not os.path.exists(path):
        print("⚠ Running trend engine...")
        run_trend_engine()

    if not os.path.exists(path):
        return {}

    with open(path) as f:
        raw = json.load(f)

    return {
        bank: {int(y): v for y, v in data["yearly_sentiment"].items()}
        for bank, data in raw.items()
    }


# ==========================================
# CORRELATION
# ==========================================

def compute_correlation(t, s):

    years = sorted(set(t.keys()) & set(y - 1 for y in s.keys()))

    if len(years) < 2:
        return None

    x, y = [], []

    for yr in years:
        if yr + 1 in s:
            x.append(t[yr])
            y.append(s[yr + 1])

    if len(x) < 2:
        return None

    return pearsonr(x, y)[0]


# ==========================================
# MAIN
# ==========================================

def main():

    print("\n🔎 Running Transformation Engine\n")

    banks = discover_banks(BASE_CORP_PATH)
    sentiments = load_sentiment_trend()

    report = []

    for bank, comp in banks.items():

        name = bank.replace("_", " ")
        print("\n🏦", name)

        annual = compute_scores_from_folder(comp["annual_reports"])
        investor = compute_scores_from_folder(comp["investor_presentations"])

        scores = annual.copy()

        for y, v in investor.items():
            scores[y] = (scores.get(y, 0) + v) / 2

        scores = normalize_scores(scores)

        corr = compute_correlation(scores, sentiments.get(name, {}))

        report.append(f"\n{name}")

        if corr is None:
            report.append("Insufficient data")
        else:
            report.append(f"Correlation: {corr:.3f}")

    with open(FINAL_OUTPUT_PATH, "w") as f:
        f.write("\n".join(report))

    print("\n📄 Report saved:", FINAL_OUTPUT_PATH)


# ==========================================
if __name__ == "__main__":
    main()