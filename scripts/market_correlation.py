import os
import json
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from PyPDF2 import PdfReader
import re

# OCR libraries
import pytesseract
from pdf2image import convert_from_path


# ==========================================
# CONFIG
# ==========================================

BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"
TREND_JSON_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/bank_trend_data.json"
OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/strategic_market_intelligence_report.txt"

TRANSFORMATION_KEYWORDS = [
    "digital","mobile","platform","ecosystem",
    "ai","automation","analytics",
    "customer experience","innovation",
    "technology","upgrade"
]


# ==========================================
# OCR EXTRACTION (FOR IMAGE PDFs)
# ==========================================

def extract_text_with_ocr(pdf_path):

    text = ""

    try:
        images = convert_from_path(pdf_path, dpi=200)

        for img in images:
            text += pytesseract.image_to_string(img)

    except Exception as e:
        print("OCR failed for:", pdf_path)

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
# DISCOVER BANKS
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
# TRANSFORMATION KEYWORD SCAN
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

            try:

                reader=PdfReader(full_path)

                pdf_text=""

                for page in reader.pages:
                    pdf_text+=page.extract_text() or ""

                # If no text extracted -> use OCR
                if len(pdf_text.strip()) < 100:

                    print("Running OCR for:",full_path)

                    pdf_text=extract_text_with_ocr(full_path)

                combined_text+=pdf_text

            except Exception as e:

                print("PDF read failed:",full_path)

                continue

        combined_text=combined_text.lower()

        keywords_found=[]

        for kw in TRANSFORMATION_KEYWORDS:

            if kw in combined_text:
                keywords_found.append(kw)

        yearly_focus[year]=keywords_found

    return yearly_focus


# ==========================================
# CORRELATION
# ==========================================

def compute_correlation(sentiment,returns,lag=0):

    aligned=[]

    for year in sentiment:

        target=year+lag

        if target in returns:

            aligned.append(
                (year,sentiment[year],returns[target])
            )

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

    sentiment_data=load_sentiment_data()

    banks=discover_banks(BASE_CORP_PATH)

    report=[]

    report.append("STRATEGIC MARKET & SENTIMENT INTELLIGENCE REPORT")
    report.append("=================================================\n")

    for bank,components in banks.items():

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

        if same_corr is not None:
            report.append(f"- Same Year Correlation: {same_corr:.3f}")

        if next_corr is not None:
            report.append(f"- Next Year Correlation: {next_corr:.3f}")

        if same_corr is None and next_corr is None:
            report.append("- Insufficient overlapping data for statistical correlation.")

        report.append("\nYear-by-Year Strategic Breakdown:")

        all_years=sorted(set(
            list(sentiment.keys())+
            list(returns.keys())+
            list(transformation.keys())
        ))

        for year in all_years:

            report.append(f"\n📅 {year}")

            focus=transformation.get(year)

            if focus:
                report.append(f"Transformation Focus: {', '.join(focus)}")
            else:
                report.append("Transformation Focus: Not identified")

            if year in sentiment:
                s=sentiment[year]
                mood="Positive" if s>0 else "Negative"
                report.append(f"Customer Sentiment: {s:.3f} ({mood})")
            else:
                report.append("Customer Sentiment: Not available")

            if year in returns:
                r=returns[year]
                direction="Positive" if r>0 else "Negative"
                report.append(f"Market Return: {r:.3f} ({direction})")
            else:
                report.append("Market Return: Not available")

            if year in sentiment and year in returns:

                s=sentiment[year]
                r=returns[year]

                if s>0 and r>0:
                    report.append("Interpretation: Strong alignment between customer perception and market performance.")
                elif s<0 and r>0:
                    report.append("Interpretation: Market confidence exists despite negative customer sentiment.")
                elif s>0 and r<0:
                    report.append("Interpretation: Positive customer perception not reflected in market valuation.")
                else:
                    report.append("Interpretation: Operational or strategic challenges reflected in both sentiment and stock.")

        report.append("\n"+"="*60)

    final_text="\n".join(report)

    with open(OUTPUT_PATH,"w") as f:
        f.write(final_text)

    print("\nReport generated successfully.")
    print("\nSaved to:",OUTPUT_PATH)


if __name__=="__main__":
    main()