import os
from scripts.corporate_topic_sentiment import CorporateTopicSentiment
from scripts.transformation_correlation import extract_text_from_pdf
from scripts.db_cache import save_corporate_topic_sentiment


BASE_CORP_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents"


def extract_year(path):

    import re

    match = re.search(r"20\d{2}", path)

    if match:
        return int(match.group())

    return None


def main():

    analyzer = CorporateTopicSentiment()

    for bank in os.listdir(BASE_CORP_PATH):

        bank_folder = os.path.join(BASE_CORP_PATH, bank)

        if not os.path.isdir(bank_folder):
            continue

        print(f"\nProcessing Corporate Topics for {bank}")

        for root, dirs, files in os.walk(bank_folder):

            for file in files:

                if not file.endswith(".pdf"):
                    continue

                path = os.path.join(root, file)

                year = extract_year(path)

                text = extract_text_from_pdf(path)

                topic_scores = analyzer.analyze(text)

                save_corporate_topic_sentiment(bank, year, topic_scores)

                print(f"{bank} {year} topics:", topic_scores)