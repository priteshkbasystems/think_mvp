import time
import sys

print("\n🚀 AI Banking Intelligence Pipeline Starting...\n")

start_time = time.time()


# ==========================================
# STEP 1 — DATA INDEXING
# ==========================================

try:
    print("STEP 1: Indexing Data\n")

    from scripts.data_indexer import main as run_indexer
    run_indexer()

except Exception as e:
    print("❌ Data Indexer Failed:", e)
    sys.exit(1)


# ==========================================
# STEP 2 — SENTIMENT TREND ANALYSIS
# ==========================================

try:
    print("\nSTEP 2: Running Sentiment Trend Engine\n")

    from trend_analysis import main as run_trend
    run_trend()

except Exception as e:
    print("❌ Sentiment Trend Engine Failed:", e)
    sys.exit(1)


# ==========================================
# STEP 3 — TRANSFORMATION CORRELATION
# ==========================================

try:
    print("\nSTEP 3: Running Transformation Intelligence\n")

    from scripts.transformation_correlation import main as run_transformation
    run_transformation()

except Exception as e:
    print("❌ Transformation Intelligence Failed:", e)
    sys.exit(1)


# ==========================================
# STEP 4 — STRATEGIC MARKET INTELLIGENCE
# ==========================================

try:
    print("\nSTEP 4: Running Strategic Market Intelligence\n")

    from scripts.strategic_market_intelligence import main as run_market
    run_market()

except Exception as e:
    print("❌ Strategic Market Intelligence Failed:", e)
    sys.exit(1)


# ==========================================
# STEP 5 — AI EXECUTIVE INSIGHTS
# ==========================================

try:
    print("\nSTEP 5: Generating AI Executive Insights\n")

    from scripts.ai_insight_generator import main as run_ai
    run_ai()

except Exception as e:
    print("❌ AI Insight Generator Failed:", e)
    sys.exit(1)


# ==========================================
# PIPELINE FINISHED
# ==========================================

end_time = time.time()

print("\n✅ PIPELINE COMPLETE\n")

print(f"⏱ Total runtime: {round(end_time - start_time,2)} seconds\n")

print("📊 Generated Reports:\n")

print("• bank_trend_report.txt")
print("• bank_trend_data.json")
print("• transformation_correlation_report.txt")
print("• strategic_market_intelligence_report.txt")
print("• executive_ai_insights.txt")

print("\n📂 Location:")
print("/content/drive/MyDrive/THINK_MVP/04_Analysis_Output\n")