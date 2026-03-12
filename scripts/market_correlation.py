import os
import json
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from PyPDF2 import PdfReader
import re
import sqlite3
import hashlib

# OCR
import pytesseract
from pdf2image import convert_from_path

# AI
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
TREND_JSON_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"
OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/strategic_market_intelligence_report.txt"

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"

MAX_SENTENCES = 300  # prevents extremely slow embedding


# TRANSFORMATION_THEMES = [
#     "digital transformation in banking",
#     "banking technology modernization",
#     "customer experience improvement",
#     "data analytics and AI in banking",
#     "automation of financial services",
#     "sustainability and ESG banking strategy",
#     "platform banking ecosystem",
#     "business model innovation in banking",
#     "portfolio optimization in banking",
#     "financial technology innovation",
#     "digital banking services expansion",
# ]
TRANSFORMATION_THEMES = [

    # Digital Transformation Core
    "digital transformation",
    "digital banking strategy",
    "digital banking",
    "digital operating model",
    "technology driven banking",
    "digital capability",
    "digital organization",

    # Customer Experience Transformation
    "digital customer experience",
    "customer journey",
    "omnichannel banking",
    "mobile banking",
    "personalized financial services",
    "digital service channels",

    # Artificial Intelligence & Data
    "artificial intelligence",
    "AI",
    "machine learning",
    "advanced analytics",
    "data analytics",
    "predictive analytics",
    "data driven",
    "data platform",
    "big data",

    # Automation & Efficiency
    "automation",
    "process automation",
    "robotic process automation",
    "RPA",
    "intelligent automation",
    "operational efficiency",
    "digitization of processes",

    # Digital Banking Platforms
    "digital platform",
    "mobile banking platform",
    "online banking platform",
    "digital banking platform",
    "next generation banking",
    "platform banking",

    # Payments Innovation
    "digital payments",
    "real time payments",
    "cashless society",
    "contactless payment",
    "QR payment",
    "mobile wallet",
    "digital payment ecosystem",

    # Fintech & Ecosystem
    "fintech",
    "fintech partnership",
    "open banking",
    "banking ecosystem",
    "platform ecosystem",
    "banking as a service",
    "API banking",
    "digital ecosystem",

    # Cloud & Infrastructure
    "cloud",
    "cloud computing",
    "cloud infrastructure",
    "core banking modernization",
    "IT modernization",
    "microservices",
    "technology infrastructure",

    # Cybersecurity & Digital Risk
    "cybersecurity",
    "digital security",
    "digital identity",
    "authentication",
    "fraud detection",

    # Innovation & R&D
    "innovation lab",
    "innovation center",
    "technology innovation",
    "digital innovation",
    "research and development",
    "venture investment",
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
    "banking transformation",
    "future of banking",
    "branch to digital",
    "technology driven bank"
]

# ==========================================
# INIT DATABASE
# ==========================================

def init_db():

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS embedding_cache (
        text_hash TEXT PRIMARY KEY,
        embedding BLOB
    )
    """)

    conn.commit()
    conn.close()


# ==========================================
# EMBEDDING CACHE
# ==========================================

def get_embedding(text):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    text_hash = hashlib.md5(text.encode()).hexdigest()

    cursor.execute(
        "SELECT embedding FROM embedding_cache WHERE text_hash=?",
        (text_hash,)
    )

    row = cursor.fetchone()
    conn.close()

    if row:
        return np.frombuffer(row[0], dtype=np.float32)

    return None


def save_embedding(text, embedding):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    text_hash = hashlib.md5(text.encode()).hexdigest()

    cursor.execute("""
    INSERT OR IGNORE INTO embedding_cache
    (text_hash, embedding)
    VALUES (?,?)
    """, (text_hash, embedding.astype(np.float32).tobytes()))

    conn.commit()
    conn.close()


# ==========================================
# LOAD MODEL
# ==========================================

print("Loading AI model...")

embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

THEME_EMBEDDINGS = embedding_model.encode(TRANSFORMATION_THEMES)

print("Model ready")


# ==========================================
# OCR
# ==========================================

def extract_text_with_ocr(pdf_path):

    text=""

    try:
        images = convert_from_path(pdf_path, dpi=200)

        for img in images:
            text += pytesseract.image_to_string(img)

    except:
        print("OCR failed:", pdf_path)

    return text


# ==========================================
# LOAD SENTIMENT
# ==========================================

def load_sentiment_data():

    if not os.path.exists(TREND_JSON_PATH):
        return {}

    with open(TREND_JSON_PATH,"r") as f:
        raw=json.load(f)

    sentiment_data={}

    for bank,data in raw.items():

        sentiment_data[bank]={
            int(year):score
            for year,score in data["yearly_sentiment"].items()
        }

    return sentiment_data


# ==========================================
# BANK DISCOVERY
# ==========================================

def discover_banks(base_path):

    banks={}

    for bank_folder in os.listdir(base_path):

        bank_path=os.path.join(base_path,bank_folder)

        if not os.path.isdir(bank_path):
            continue

        display_name=bank_folder.replace("_"," ")

        stock_path=os.path.join(bank_path,"stock_price")

        annual_path=None

        for sub in os.listdir(bank_path):

            sub_path=os.path.join(bank_path,sub)

            if not os.path.isdir(sub_path):
                continue

            if sub.lower()=="annual_reports":
                annual_path=sub_path

        stock_file=None

        if os.path.exists(stock_path):

            for file in os.listdir(stock_path):

                if file.endswith(".xlsx"):
                    stock_file=os.path.join(stock_path,file)

        banks[display_name]={
            "stock":stock_file,
            "annual":annual_path
        }

    return banks


# ==========================================
# STOCK RETURNS
# ==========================================

def compute_yearly_returns(csv_path):

    df=pd.read_csv(csv_path)

    df["Date"]=pd.to_datetime(df["Date"],errors="coerce")

    df=df.dropna(subset=["Date"])

    df["Price"]=df["Price"].astype(str).str.replace(",","").astype(float)

    df["Year"]=df["Date"].dt.year

    yearly_returns={}

    for year in df["Year"].unique():

        year_df=df[df["Year"]==year].sort_values("Date")

        if len(year_df)<2:
            continue

        first_price=year_df.iloc[0]["Price"]
        last_price=year_df.iloc[-1]["Price"]

        yearly_returns[int(year)]=(last_price-first_price)/first_price

    return yearly_returns


# ==========================================
# TRANSFORMATION SCORE
# ==========================================

def compute_transformation_score(text):

    sentences = re.split(r"[.!?]", text)

    sentences = [s.strip() for s in sentences if len(s.strip()) > 30]

    sentences = sentences[:MAX_SENTENCES]   # speed limit

    if len(sentences) == 0:
        return 0

    sentence_embeddings=[]

    for sentence in sentences:

        emb = get_embedding(sentence)

        if emb is None:

            emb = embedding_model.encode([sentence])[0]

            save_embedding(sentence, emb)

        sentence_embeddings.append(emb)

    sentence_embeddings=np.array(sentence_embeddings)

    similarity_matrix = cosine_similarity(sentence_embeddings, THEME_EMBEDDINGS)

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
# PDF EXTRACTION
# ==========================================

def extract_transformation_focus(folder_path):

    yearly_focus={}

    if not folder_path:
        return yearly_focus

    for root,dirs,files in os.walk(folder_path):

        year_match=re.search(r"(20\d{2})",root)

        if not year_match:
            continue

        year=int(year_match.group(1))

        combined_text=""

        for file in files:

            if not file.endswith(".pdf"):
                continue

            full_path=os.path.join(root,file)

            print("Processing PDF:",file)

            try:

                reader=PdfReader(full_path)

                pdf_text=""

                for page in reader.pages:
                    pdf_text+=page.extract_text() or ""

                if len(pdf_text.strip()) < 100:

                    print("Running OCR:",file)

                    pdf_text=extract_text_with_ocr(full_path)

                combined_text+=pdf_text

            except:

                print("Failed:",file)

        combined_text=combined_text.lower()

        score = compute_transformation_score(combined_text)

        yearly_focus[year]=score

    return yearly_focus


# ==========================================
# CORRELATION
# ==========================================

def compute_correlation(sentiment,returns,lag=0):

    aligned=[]

    for year in sentiment:

        target=year+lag

        if target in returns:

            aligned.append((year,sentiment[year],returns[target]))

    if len(aligned)<2:
        return None

    x=[i[1] for i in aligned]
    y=[i[2] for i in aligned]

    corr,_=pearsonr(x,y)

    return corr


# ==========================================
# MAIN
# ==========================================

def main():

    init_db()

    sentiment_data=load_sentiment_data()

    banks=discover_banks(BASE_CORP_PATH)

    report=[]

    report.append("STRATEGIC MARKET & SENTIMENT INTELLIGENCE REPORT")
    report.append("=================================================\n")

    for bank,components in banks.items():

        print("\nAnalyzing:",bank)

        report.append(f"\n🏦 {bank}")
        report.append("-"*(len(bank)+3))

        sentiment=sentiment_data.get(bank,{})
        returns=compute_yearly_returns(components["stock"]) if components["stock"] else {}
        transformation=extract_transformation_focus(components["annual"])

        if not sentiment:
            report.append("No sentiment data available.\n")
            continue

        same_corr=compute_correlation(sentiment,returns,lag=0)
        next_corr=compute_correlation(sentiment,returns,lag=1)

        report.append("\nExecutive Summary:")

        if same_corr:
            report.append(f"- Same Year Correlation: {same_corr:.3f}")

        if next_corr:
            report.append(f"- Next Year Correlation: {next_corr:.3f}")

        report.append("\nYear-by-Year Strategic Breakdown:")

        for year in sorted(transformation.keys()):

            report.append(f"\n📅 {year}")
            report.append(f"Transformation Intensity: {transformation[year]:.3f}")

        report.append("\n"+"="*60)


    final_text="\n".join(report)

    with open(OUTPUT_PATH,"w") as f:
        f.write(final_text)

    print("\nReport generated successfully")
    print("Saved to:",OUTPUT_PATH)


if __name__=="__main__":
    main()