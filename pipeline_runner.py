import time
import sys
import sqlite3

print("\n🚀 AI Banking Intelligence Pipeline Starting...\n")

start_time = time.time()

DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"

from scripts.pipeline_manager import PipelineManager
from scripts.pipeline_dependency_manager import PipelineDependencyManager

pipeline = PipelineManager()
dep_manager = PipelineDependencyManager()


# ===============================
# FORCE FULL REBUILD
# ===============================

if "--rebuild" in sys.argv:

    print("⚠ Forcing full rebuild of pipeline\n")

    pipeline.cursor.execute("DELETE FROM pipeline_runs")

    pipeline.conn.commit()


# ===============================
# PIPELINE STEP EXECUTOR
# ===============================

def run_step(step_name, func):

    try:

        print(f"\n{step_name}")

        if dep_manager.dependency_changed(step_name):

            print("Dependency changed → rerunning step")

            func()

            pipeline.mark_success(step_name)

            return

        if pipeline.should_run(step_name):

            func()

            pipeline.mark_success(step_name)

        else:

            print("✔ Skipped (cached)")

    except Exception as e:

        pipeline.mark_failed(step_name)

        print(f"❌ {step_name} Failed:", e)

        sys.exit(1)


# ==========================================
# STEP 1 — DATA INDEXING
# ==========================================

def step1():
    from scripts.data_indexer import main
    main()

run_step("STEP 1 — DATA INDEXING", step1)


# ==========================================
# STEP 2 — SENTIMENT TREND ANALYSIS
# ==========================================

def step2():
    from trend_analysis import main
    main()

run_step("STEP 2 — SENTIMENT TREND", step2)


# ==========================================
# STEP 3 — TRANSFORMATION INTELLIGENCE
# ==========================================

def step3():
    from scripts.transformation_correlation import main
    main()

run_step("STEP 3 — TRANSFORMATION INTELLIGENCE", step3)


# ==========================================
# STEP 4 — CORPORATE TOPIC SENTIMENT
# ==========================================

def step4():
    from scripts.corporate_topic_pipeline import main
    main()

run_step("STEP 4 — CORPORATE TOPIC SENTIMENT", step4)


# ==========================================
# STEP 5 — NARRATIVE SCORE GENERATION
# ==========================================

def step5():
    from scripts.narrative_score_generator import generate_narrative_scores
    generate_narrative_scores()

run_step("STEP 5 — NARRATIVE SCORES", step5)


# ==========================================
# STEP 6 — TOPIC ALIGNMENT
# ==========================================

def step6():

    from scripts.topic_alignment import TopicAlignmentEngine

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT topic FROM corporate_topic_sentiment")
    corporate_topics = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT topic_id FROM complaint_topics")
    customer_topics = [r[0] for r in cursor.fetchall()]

    engine = TopicAlignmentEngine()

    alignments = engine.align_topics(corporate_topics, customer_topics)

    print("\nTopic Alignments:")
    for a in alignments:
        print(a)

    conn.close()

run_step("STEP 6 — TOPIC ALIGNMENT", step6)


# ==========================================
# STEP 7 — ASPECT SENTIMENT
# ==========================================

def step7():

    from scripts.aspect_sentiment import AspectSentimentAnalyzer

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT review_text, rating
    FROM review_sentiments
    LIMIT 500
    """)

    rows = cursor.fetchall()

    texts = [r[0] for r in rows]
    ratings = [r[1] for r in rows]

    analyzer = AspectSentimentAnalyzer()

    results = analyzer.analyze(texts, ratings)

    print("\nAspect Sentiment Results:")
    print(results)

    conn.close()

run_step("STEP 7 — ASPECT SENTIMENT", step7)


# ==========================================
# STEP 7.5 — CUSTOM SENTIMENT TAXONOMY
# ==========================================

def step75():

    from scripts.sentiment_taxonomy_pipeline import main
    main()

run_step("STEP 7.5 — CUSTOM SENTIMENT TAXONOMY", step75)


# ==========================================
# STEP 8 — DASHBOARD DATA ENGINE
# ==========================================

def step8():
    from scripts.dashboard_data_engine import main
    main()

run_step("STEP 8 — DASHBOARD DATA ENGINE", step8)


# ==========================================
# STEP 9 — STRATEGIC MARKET INTELLIGENCE
# ==========================================

def step9():
    from scripts.strategic_market_intelligence import main
    main()

run_step("STEP 9 — MARKET INTELLIGENCE", step9)


# ==========================================
# STEP 10 — AI EXECUTIVE INSIGHTS
# ==========================================

def step10():
    from scripts.ai_insight_generator import main
    main()

run_step("STEP 10 — AI EXECUTIVE INSIGHTS", step10)


# ==========================================
# STEP 11 — SCENARIO SIMULATION
# ==========================================

def step11():

    from scripts.scenario_simulator import ScenarioSimulator

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    simulator = ScenarioSimulator()

    cursor.execute("SELECT DISTINCT bank_name FROM narrative_scores")
    banks = [r[0] for r in cursor.fetchall()]

    for bank in banks:

        print(f"\nRunning Scenario Simulation for {bank}")

        cursor.execute("SELECT year, score FROM narrative_scores WHERE bank_name=?", (bank,))
        narrative_rows = cursor.fetchall()

        cursor.execute("SELECT year, sentiment FROM sentiment_scores WHERE bank_name=?", (bank,))
        sentiment_rows = cursor.fetchall()

        narrative_scores = {y: s for y, s in narrative_rows}
        sentiment_scores = {y: s for y, s in sentiment_rows}

        predict_fn = simulator.simulate(narrative_scores, sentiment_scores)

        if predict_fn:

            future_score = max(narrative_scores.values()) * 1.1
            prediction = predict_fn(future_score)

            print(f"Predicted Future Sentiment for {bank}: {round(prediction,3)}")

    conn.close()

run_step("STEP 11 — SCENARIO SIMULATION", step11)


# ==========================================
# STEP 12 — TRANSFORMATION IMPACT SCORE
# ==========================================

def step12():

    from scripts.transformation_impact_score import TransformationImpactScore

    tis = TransformationImpactScore()

    results = tis.calculate_tis()

    print("\nTransformation Impact Scores:")

    for bank, score in results.items():
        print(f"{bank}: {score}")

run_step("STEP 12 — TRANSFORMATION IMPACT SCORE", step12)


# ==========================================
# STEP 13 — SOURCE SENTIMENT CONCORDANCE
# ==========================================

def step13():

    from scripts.source_concordance_pipeline import main
    main()

run_step("STEP 13 — SOURCE SENTIMENT CONCORDANCE", step13)


# ==========================================
# STEP 14 — TRANSFORMATION LEXICON
# ==========================================

def step14():

    from scripts.transformation_lexicon import TransformationLexicon

    lex = TransformationLexicon()

    print("Loaded Transformation Terms:", len(lex.get_terms()))

run_step("STEP 14 — TRANSFORMATION LEXICON", step14)


# ==========================================
# STEP 15 — TOPIC SENTIMENT CORRELATION
# ==========================================

def step15():

    from scripts.topic_sentiment_correlation_pipeline import main
    main()

run_step("STEP 15 — TOPIC SENTIMENT CORRELATION", step15)


# ==========================================
# STEP 16 — CUSTOMER JOURNEY SENTIMENT
# ==========================================

def step16():

    from scripts.journey_sentiment_pipeline import main
    main()

run_step("STEP 16 — CUSTOMER JOURNEY SENTIMENT", step16)


# ==========================================
# STEP 17 — HUMAN FEEDBACK LOOP
# ==========================================

def step17():

    from scripts.feedback_learning import FeedbackLearning

    f = FeedbackLearning()

    print("Human labeled samples:", f.check_new_labels())

run_step("STEP 17 — HUMAN FEEDBACK LOOP", step17)


# ==========================================
# STEP 18 — MODEL RETRAINING
# ==========================================

def step18():

    from scripts.model_retraining import ModelRetraining

    r = ModelRetraining()

    r.retrain()

run_step("STEP 18 — MODEL RETRAINING", step18)


# ==========================================
# STEP 19 — TRANSFORMATION COMPETENCIES
# ==========================================

def step19():

    from scripts.transformation_competency_engine import TransformationCompetencyEngine

    engine = TransformationCompetencyEngine()

    engine.compute()

run_step("STEP 19 — TRANSFORMATION COMPETENCIES", step19)


# ==========================================
# STEP 20 — TRANSFORMATION PERFORMANCE INDEX
# ==========================================

def step20():

    from scripts.transformation_performance_index import TransformationPerformanceIndex

    engine = TransformationPerformanceIndex()

    scores = engine.compute()

    print("\nTransformation Performance Index")

    for bank, score in scores.items():
        print(bank, score)

run_step("STEP 20 — TRANSFORMATION PERFORMANCE INDEX", step20)


# ==========================================
# STEP 21 — COMPETITOR BENCHMARK
# ==========================================

def step21():

    from scripts.competitor_benchmark_pipeline import main
    main()

run_step("STEP 21 — COMPETITOR BENCHMARK", step21)


# ==========================================
# STEP 22 — CONVERSATION SENTIMENT FLOW
# ==========================================

def step22():

    from scripts.conversation_sentiment_pipeline import main
    main()

run_step("STEP 22 — CONVERSATION SENTIMENT FLOW", step22)


# ==========================================
# STEP 23 — CORPORATE SENTIMENT MODEL
# ==========================================

def step23():

    from scripts.corporate_sentiment_pipeline import main
    main()

run_step("STEP 23 — CORPORATE SENTIMENT MODEL", step23)


# ==========================================
# STEP 24 — SUCCESS FACTOR DETECTION
# ==========================================

def step24():

    from scripts.success_factor_pipeline import main
    main()

run_step("STEP 24 — SUCCESS FACTOR DETECTION", step24)


# ==========================================
# STEP 25 — TRANSFORMATION NARRATIVE EVOLUTION
# ==========================================

def step25():

    from scripts.narrative_evolution_pipeline import main
    main()

run_step("STEP 25 — TRANSFORMATION NARRATIVE EVOLUTION", step25)


# ==========================================
# STEP 26 — TRANSFORMATION LAG ANALYSIS
# ==========================================

def step26():

    from scripts.transformation_lag_pipeline import main
    main()

run_step("STEP 26 — TRANSFORMATION LAG ANALYSIS", step26)

# ==========================================
# STEP 27 — TOPIC MAPPING ENGINE
# ==========================================

def step27():

    from scripts.topic_mapping_engine import TopicMappingEngine

    engine = TopicMappingEngine()
    engine.run()

run_step("STEP 27 — TOPIC MAPPING", step27)

# ==========================================
# STEP 28 — FINANCIAL METRICS EXTRACTION
# ==========================================

def step28():

    from scripts.financial_extraction_pipeline import main

    main()

run_step("STEP 28 — FINANCIAL METRICS EXTRACTION", step28)
# ==========================================
# PIPELINE FINISHED
# ==========================================

end_time = time.time()

print("\n✅ PIPELINE COMPLETE\n")

print(f"⏱ Total runtime: {round(end_time - start_time,2)} seconds\n")

print("\n📂 Output location:")
print("/content/drive/MyDrive/THINK_MVP/04_Analysis_Output\n")