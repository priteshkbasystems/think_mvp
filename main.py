from scripts.input_handler import get_sample_text
from scripts.processor import TextProcessor

if __name__ == "__main__":
    print("🚀 Think MVP Pipeline Started")

    texts = get_sample_text()

    processor = TextProcessor()
    results = processor.process(texts)

    for r in results:
        print(r)