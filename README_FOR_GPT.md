## Current Flow Reference

This file reflects the current active pipeline in `pipeline_runner.py` after cleanup/removals.

## Active Pipeline Steps

1. `STEP 1 — DATA INDEXING` -> `scripts.data_indexer.main`
2. `STEP 2 — SENTIMENT TREND` -> `trend_analysis.main`
3. `STEP 4 — CORPORATE TOPIC SENTIMENT` -> `scripts.corporate_topic_pipeline.main`
4. `STEP 5 — NARRATIVE SCORES` -> `scripts.narrative_score_generator.generate_narrative_scores`
5. `STEP 6 — TOPIC ALIGNMENT` -> `scripts.topic_alignment.TopicAlignmentEngine`
6. `STEP 7 — ASPECT SENTIMENT` -> `scripts.aspect_sentiment.AspectSentimentAnalyzer`
7. `STEP 8 — DASHBOARD DATA ENGINE` -> `scripts.dashboard_data_engine.main`
8. `STEP 11 — SCENARIO SIMULATION` -> `scripts.scenario_simulator.ScenarioSimulator`
9. `STEP 12 — TRANSFORMATION IMPACT SCORE` -> `scripts.transformation_impact_score.TransformationImpactScore`
10. `STEP 13 — SOURCE SENTIMENT CONCORDANCE` -> `scripts.source_concordance_pipeline.main`
11. `STEP 15 — TOPIC SENTIMENT CORRELATION` -> `scripts.topic_sentiment_correlation_pipeline.main`
12. `STEP 16 — CUSTOMER JOURNEY SENTIMENT` -> `scripts.journey_sentiment_pipeline.main`
13. `STEP 19 — TRANSFORMATION COMPETENCIES` -> `scripts.transformation_competency_engine.TransformationCompetencyEngine`
14. `STEP 20 — TRANSFORMATION PERFORMANCE INDEX` -> `scripts.transformation_performance_index.TransformationPerformanceIndex`
15. `STEP 23 — CORPORATE SENTIMENT MODEL` -> `scripts.corporate_sentiment_pipeline.main`
16. `STEP 24 — SUCCESS FACTOR DETECTION` -> `scripts.success_factor_pipeline.main`
17. `STEP 26 — TRANSFORMATION LAG ANALYSIS` -> `scripts.transformation_lag_pipeline.main`
18. `STEP 27 — TOPIC MAPPING` -> `scripts.topic_mapping_engine.TopicMappingEngine`
19. `STEP 28 — FINANCIAL METRICS EXTRACTION` -> `scripts.financial_extraction_pipeline.main`

## Pipeline Control

- `scripts/pipeline_manager.py` manages `pipeline_runs` cache state.
- `scripts/pipeline_dependency_manager.py` enforces dependency-triggered reruns.

## PDF Extraction in Current Flow

PDFs from `annual_reports` and `investor_presentations` are processed mainly in:

- `scripts/corporate_topic_pipeline.py`
- `scripts/corporate_sentiment_model.py`
- `scripts/financial_extraction.py`

Current extraction stack:

- `PyPDF2.PdfReader`
- `pdfplumber` (financial extraction path)
- OCR fallback: `pdf2image.convert_from_path` + `pytesseract.image_to_string`

## Key Database Tables Used by Active Flow

- `banks`
- `stock_returns`
- `review_sentiments`
- `sentiment_scores`
- `pdf_cache`
- `pdf_text_cache`
- `corporate_topic_sentiment`
- `corporate_sentiment`
- `narrative_scores`
- `narrative_sentiment_correlation`
- `narrative_lag`
- `sentiment_predictions`
- `narrative_highlights`
- `source_concordance`
- `topic_sentiment_correlation`
- `journey_sentiment`
- `success_factors`
- `transformation_competencies`
- `transformation_impact_scores`
- `transformation_performance_index`
- `transformation_lag_results`
- `financial_metrics`
- `pipeline_runs`
- `step_progress`

## Current Table Columns (from `scripts/db_cache.py`)

- `banks`: `bank_name`, `color`, `bank_id` (added via migration helper)
- `stock_returns`: `bank_name`, `year`, `return`, `bank_id` (migration)
- `financial_metrics`: `bank_name`, `year`, `revenue`, `net_profit`, `operating_income`, `total_assets`, `roe`, `bank_id` (migration)
- `financial_statement_sheets`: `bank_name`, `year`, `file_path`, `sheet_name`, `payload_json`, `bank_id` (migration)
- `review_sentiments`: `id`, `bank_name`, `year`, `review_text`, `review_hash`, `rating`, `sentiment_score`, `sentiment_label`, `topic_id`, `review_source`, `bank_id` (migration)
- `sentiment_scores`: `bank_name`, `year`, `sentiment`, `contradiction_ratio`, `bank_id` (migration)
- `sentiment_predictions`: `bank_name`, `year`, `predicted_sentiment`, `bank_id` (migration)
- `complaint_topics`: `id`, `bank_name`, `topic_id`, `keywords`, `review_count`, `created_at`, `bank_id` (migration)
- `corporate_topic_sentiment`: `bank_name`, `year`, `topic`, `sentiment`, `bank_id` (migration)
- `corporate_topic_cache`: `file_path`, `last_modified`
- `pdf_cache`: `file_path`, `last_modified`, `year`, `score`
- `pdf_text_cache`: `file_path`, `text`
- `embedding_cache`: `id`, `text_hash`, `embedding`
- `transformation_competencies`: `id`, `bank_name`, `year`, `competency`, `score`, `bank_id` (migration)
- `transformation_impact_scores`: `bank_id`, `bank_name`, `tis_score`, `updated_at`
- `transformation_performance_index`: `bank_id`, `bank_name`, `score`, `updated_at`
- `transformation_lag_results`: `bank_id`, `bank_name`, `lag_years`, `correlation`, `updated_at`
- `narrative_scores`: `bank_name`, `year`, `score`, `bank_id` (migration)
- `narrative_sentiment_correlation`: `bank_name`, `correlation`, `bank_id` (migration)
- `narrative_lag`: `bank_name`, `lag_months`, `bank_id` (migration)
- `narrative_highlights`: `id`, `bank_name`, `year`, `highlight`, `bank_id` (migration)
- `source_concordance`: `id`, `bank_name`, `review_source`, `avg_sentiment`, `bank_id` (migration)
- `topic_sentiment_correlation`: `bank_id`, `bank_name`, `correlation`, `updated_at`
- `journey_sentiment`: `stage`, `sentiment`, `updated_at`
- `success_factors`: `bank_id`, `bank_name`, `topic_id`, `keywords`, `sentiment`, `volume`, `updated_at`
- `pipeline_runs`: `step_name`, `last_run`, `status`
- `step_progress`: `step_name`, `bank_name`, `year`, `last_processed_index`, `bank_id` (migration)
- `corporate_sentiment`: `bank_name`, `year`, `sentiment`, `bank_id` (migration)
- `corporate_sentence_sentiment`: `bank_name`, `year`, `file_path`, `sentence_index`, `page_number`, `sentence_text`, `sentiment_label`, `sentiment_score`, `signed_score`, `utterance_kind`, `topic`, `label`, `bank_id` (migration and inserts)
- `corporate_page_sentiment`: `bank_name`, `year`, `file_path`, `page_number`, `mean_signed`, `sentence_count`, `label`, `bank_id` (migration and inserts)
- `corporate_document_sentiment_rollup`: `bank_name`, `year`, `file_path`, `doc_mean_signed`, `label`, `bank_id` (migration and inserts)

Legacy tables/scripts previously removed are intentionally not documented here.

