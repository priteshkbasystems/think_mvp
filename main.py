import os
from scripts.processor import TextProcessor
import json

DATA_PATH = "/content/drive/MyDrive/THINK_MVP/ 01_Corporate_Documents/SCBX_CardX/Reviews"

all_texts = []

for root, dirs, files in os.walk(DATA_PATH):
    for file in files:
        if file.endswith(".txt"):
            with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                all_texts.append(f.read())

processor = TextProcessor()
results = processor.process(all_texts)

for r in results[:5]:
    print(r)

output_path = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/scbx_analysis.json"

with open(output_path, "w") as f:
    json.dump(results, f, indent=4)

print("✅ Analysis saved to Drive")