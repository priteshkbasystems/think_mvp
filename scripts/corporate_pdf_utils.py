import os

from PyPDF2 import PdfReader
import pytesseract
from pdf2image import convert_from_path

from scripts.db_cache import get_cached_pdf_text, save_pdf_text


ALLOWED_CORPORATE_FOLDERS = {"annual_reports", "investor_presentations", "investors_presentations"}
BLOCKED_FOLDERS = {"financial_report", "reviews", "stock_price"}


def discover_bank_corporate_folders(base_path):
    banks = {}
    if not os.path.exists(base_path):
        return banks

    for bank_folder in os.listdir(base_path):
        bank_path = os.path.join(base_path, bank_folder)
        if not os.path.isdir(bank_path):
            continue

        components = {
            "annual_reports": None,
            "investor_presentations": None,
        }

        for sub in os.listdir(bank_path):
            sub_path = os.path.join(bank_path, sub)
            if not os.path.isdir(sub_path):
                continue
            sub_lower = sub.lower()
            if sub_lower == "annual_reports":
                components["annual_reports"] = sub_path
            elif sub_lower in {"investor_presentations", "investors_presentations"}:
                components["investor_presentations"] = sub_path

        banks[bank_folder] = components
    return banks


def is_allowed_corporate_pdf(file_path):
    norm = (file_path or "").replace("\\", "/").strip("/")
    parts = [p.lower() for p in norm.split("/") if p]
    if any(p in BLOCKED_FOLDERS for p in parts):
        return False
    return any(p in ALLOWED_CORPORATE_FOLDERS for p in parts)


def extract_text_with_ocr(pdf_path):
    text = ""
    try:
        images = convert_from_path(pdf_path, dpi=200)
        for img in images:
            text += pytesseract.image_to_string(img)
    except Exception:
        print("OCR failed:", pdf_path)
    return text.lower()


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
    except Exception:
        print("❌ Failed to read PDF:", pdf_path)
        return ""


def extract_pdf_pages(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        pages = []
        for i, page in enumerate(reader.pages):
            t = page.extract_text() or ""
            if t.strip():
                pages.append((i + 1, t))
        full = "".join(p[1] for p in pages)
        if len(full.strip()) < 100:
            ocr = extract_text_with_ocr(pdf_path)
            if ocr.strip():
                return [(1, ocr)]
            return []
        return pages
    except Exception:
        return []
