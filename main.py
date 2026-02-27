from scripts.processor import TextProcessor
import os
import json
import pandas as pd

# =====================================
# CONFIGURATION
# =====================================

DATA_PATH = "/content/drive/MyDrive/THINK_MVP/01_Corporate_Documents/Krungthai Bank/Reviews"
OUTPUT_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/krungthai_analysis.json"

# =====================================
# VALIDATE PATH
# =====================================

print("🔍 Checking data path...")
print("Path:", DATA_PATH)

if not os.path.exists(DATA_PATH):
    print("❌ ERROR: Path does not exist. Please check folder name carefully.")
    exit()

# =====================================
# LOAD EXCEL FILES
# =====================================

all_texts = []

print("\n📂 Scanning for Excel files...\n")

for root, dirs, files in os.walk(DATA_PATH):
    for file in files:
        if file.endswith(".xlsx"):
            file_path = os.path.join(root, file)
            print(f"📄 Loading: {file_path}")

            try:
                df = pd.read_excel(file_path)
            except Exception as e:
                print(f"   ⚠ Failed to read file: {e}")
                continue

            print("   Columns detected:", df.columns.tolist())

            # Try common review column names
            possible_columns = [
                "review", "Review",
                "comment", "Comment",
                "feedback", "Feedback",
                "text", "Text",
                "content", "Content"
            ]

            review_column = None
            for col in possible_columns:
                if col in df.columns:
                    review_column = col
                    break

            if review_column is None:
                print("   ⚠ No review column found. Skipping file.")
                continue

            texts = df[review_column].dropna().astype(str).tolist()
            all_texts.extend(texts)

print("\n📊 Total texts loaded:", len(all_texts))

# =====================================
# SAFETY CHECK
# =====================================

if len(all_texts) == 0:
    print("❌ No texts found. Cannot run analysis.")
    exit()

# =====================================
# RUN AI PIPELINE
# =====================================

print("\n🚀 Running Think MVP Pipeline...\n")

processor = TextProcessor()
results = processor.process(all_texts)

# =====================================
# SAVE OUTPUT
# =====================================

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

print("\n✅ Analysis saved to:", OUTPUT_PATH)

# =====================================
# SHOW SAMPLE RESULTS
# =====================================

print("\n🔎 Sample Results:")
for r in results[:5]:
    print(r)