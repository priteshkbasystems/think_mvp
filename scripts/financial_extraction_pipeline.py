from scripts.financial_extraction import FinancialExtractor

def main():
    print("🔥 NEW FINANCIAL PIPELINE LOADED 🔥")
    extractor = FinancialExtractor()

    extractor.run()