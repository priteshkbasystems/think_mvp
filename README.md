## AI Banking Intelligence Platform – Overview

This document is written so that ChatGPT (or any LLM) can quickly understand the overall flow of the platform: folders, key files, database schema, and which models are used for which purpose.

---

## 1. Folder structure (high level)

- **Project root**
  - `pipeline_runner.py`: Orchestrates the full multi-step analytics pipeline (entry point).
  - `main.py`: Simple demo/entry for running sentiment processing on tabular review data.
  - `benchmark.py`: Benchmarking / comparison utilities using the core processor and root-cause engine.
  - `trend_analysis.py`: Yearly bank sentiment trend engine (reads review spreadsheets, outputs trend report/JSON).
  - `requirements.txt`: Python dependencies.
- **models/**
  - `sentiment_model.py`: Wraps Hugging Face sentiment pipeline.
  - `embedding_model.py`: Wraps SentenceTransformer for text embeddings.
  - `topic_model.py`: Wraps KMeans for topic/cluster assignments.
- **scripts/**
  - `processor.py`: Core text processing engine (sentiment + rating fusion, topics, keywords, summary).
  - `root_cause_analyzer.py`: Explains *why* sentiment is high/low (aggregates issues, contradictions, etc.).
  - `db_cache.py`: Central SQLite schema + caching helpers for scores, reviews, embeddings, and dashboard tables.
  - `dashboard_data_engine.py`: Reads from the cache DB and generates topic sentiment, correlations, lags, predictions, highlights for dashboards.
  - `data_indexer.py`: Indexes raw source data into a consistent structure for downstream steps.
  - `transformation_correlation.py`: Links transformation/narrative measures with sentiment and stock metrics.
  - `narrative_score_generator.py`: Generates narrative / transformation scores and stores them.
  - `ai_insight_generator.py`: High-level AI-generated narrative / executive insights, using cached metrics.
  - `strategic_market_intelligence.py`: Wraps market and transformation analytics into a report.
  - `topic_discovery.py`: Additional topic / theme discovery logic (beyond simple KMeans topics).
  - `model_manager.py`: Central utilities for loading / managing models and re-use.
  - `env_check.py`, `input_handler.py`, `test_module.py`, `test_sentiments.py`: Utility / testing / environment-check scripts.
  - `scripts/utils/`
    - `sentiment_utils.py`: Utilities to combine text sentiment with star ratings and classify final sentiment labels.

> When in doubt, consider `pipeline_runner.py` as the canonical description of pipeline order, and `db_cache.py` + `dashboard_data_engine.py` as the canonical definition of what is persisted for dashboards.

---

## 2. File-level flow (main execution paths)

### 2.1 End-to-end pipeline (`pipeline_runner.py`)

High-level steps:

1. **Data Indexing** – `scripts.data_indexer.main`
   - Normalizes and indexes input review/financial/corporate-report sources into a prepared structure under the configured Google Drive paths.
2. **Sentiment Trend Analysis** – `trend_analysis.main`
   - Loads yearly customer review data per bank from Excel.
   - Uses `scripts.processor.TextProcessor` to get an *overall yearly sentiment* score.
   - Writes:
     - `bank_trend_report.txt`
     - `bank_trend_data.json` (used later by downstream market/sentiment steps).
3. **Transformation Intelligence** – `scripts.transformation_correlation.main`
   - Analyzes transformation/narrative metrics vs sentiment/stock performance.
   - Writes correlation/insight reports and/or caches results in SQLite.
4. **Corporate Narrative Score** – `scripts.narrative_score_generator.generate_narrative_scores`
   - Computes per-bank *narrative / transformation* scores (e.g., based on corporate PDFs).
   - Stores them in the SQLite DB (`narrative_scores` table in `db_cache.py`).
5. **Dashboard Data Engine** – `scripts.dashboard_data_engine.main`
   - Reads from SQLite (`review_sentiments`, `narrative_scores`, `sentiment_scores`, `pdf_cache`, etc.).
   - Materializes:
     - Topic sentiment aggregates.
     - Narrative–sentiment correlations.
     - Narrative lag metrics.
     - Basic linear sentiment forecasts.
     - Narrative highlights.
   - Writes all of the above into specific dashboard tables (see DB schema below).
6. **Strategic Market Intelligence** – `scripts.strategic_market_intelligence.main`
   - High-level report combining:
     - Sentiment trends.
     - Stock returns.
     - Transformation intensity from corporate PDFs.
   - Saves `strategic_market_intelligence_report.txt`.
7. **AI Executive Insights** – `scripts.ai_insight_generator.main`
   - Consumes DB tables and/or generated text reports.
   - Produces `executive_ai_insights.txt`, summarising key risks, opportunities, and trends.

### 2.2 Core sentiment & topic engine (`scripts/processor.py`)

`TextProcessor.process(texts, ratings=None)` does:

1. **Input safety**: if `texts` is empty, returns empty outputs and zero metrics.
2. **Ratings handling**:
   - If `ratings` is `None`, default to neutral rating **3** for all reviews.
3. **Model calls**:
   - `SentimentModel.predict_batch(texts)` → raw text-based sentiment (Hugging Face).
   - `EmbeddingModel.encode(texts)` → dense embeddings for each review.
   - `TopicModel.fit_predict(embeddings)` → assigns each review to a topic cluster.
4. **Rating + text fusion**:
   - Per review:
     - Convert HF sentiment label/score into signed text sentiment.
     - Call `sentiment_utils.analyze_sentiment(text_sentiment, rating)` to fuse rating and text sentiment into a final score + label.
5. **Aggregation**:
   - Compute:
     - Cluster-level average sentiment and volumes.
     - Overall sentiment score (mean of all fused scores).
6. **Keyword extraction**:
   - For each cluster with at least 2 reviews:
     - Use `TfidfVectorizer` to extract top keywords.
7. **Outputs**:
   - `results`: per-review dicts with text, rating, raw HF confidence, final_score, label, and topic cluster.
   - `executive_summary`: multi-line text summary with cluster stats and strategic insight.
   - `benchmark_data`: compact metrics (`total_reviews`, `overall_sentiment`).

This processor is reused by:

- `main.py`
- `benchmark.py`
- `trend_analysis.py`
- parts of the dashboard / narrative pipelines (e.g., via cached `review_sentiments`).

---

## 3. Database structure (SQLite – `transformation_cache.db`)

All DB access helpers live in `scripts/db_cache.py`. `DB_PATH` is:

- `/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db`

Key tables created in `init_db()`:

- **`pdf_cache`**
  - `file_path` (TEXT, PK)
  - `last_modified` (REAL)
  - `year` (INTEGER)
  - `score` (REAL) – transformation / narrative-related score for that document/year.
- **`banks`**
  - `bank_name` (TEXT, PK)
- **`sentiment_scores`**
  - `bank_name` (TEXT)
  - `year` (INTEGER)
  - `sentiment` (REAL) – aggregated sentiment (usually from `TextProcessor`).
  - `contradiction_ratio` (REAL) – measure of contradictory feedback.
  - **PK**: (`bank_name`, `year`)
- **`stock_returns`**
  - `bank_name` (TEXT)
  - `year` (INTEGER)
  - `return` (REAL) – yearly stock return.
  - **PK**: (`bank_name`, `year`)
- **`review_sentiments`**
  - `id` (INTEGER, PK AUTOINCREMENT)
  - `bank_name` (TEXT)
  - `year` (INTEGER)
  - `review_text` (TEXT)
  - `rating` (REAL) – original star rating.
  - `sentiment_score` (REAL) – fused score from text + rating.
  - `sentiment_label` (TEXT) – final label (“Positive”, “Negative”, “Neutral”).
- **`complaint_topics`**
  - `id` (INTEGER, PK AUTOINCREMENT)
  - `bank_name` (TEXT)
  - `topic_id` (INTEGER or TEXT, depending on the producer)
  - `keywords` (TEXT) – comma-separated top terms.
  - `review_count` (INTEGER)
  - `created_at` (TIMESTAMP, default `CURRENT_TIMESTAMP`)
- **`embedding_cache`**
  - `id` (INTEGER, PK AUTOINCREMENT)
  - `text_hash` (TEXT, UNIQUE) – MD5 hash of text.
  - `embedding` (BLOB) – serialized float32 vector.
  - `created_at` (TIMESTAMP, default `CURRENT_TIMESTAMP`)
- **`narrative_scores`**
  - `bank_name` (TEXT)
  - `year` (INTEGER)
  - `score` (REAL) – narrative / transformation intensity.
  - **PK**: (`bank_name`, `year`)
- **`narrative_sentiment_correlation`**
  - `bank_name` (TEXT, PK)
  - `correlation` (REAL) – Pearson r between narrative_scores and sentiment_scores.
- **`narrative_lag`**
  - `bank_name` (TEXT, PK)
  - `lag_months` (INTEGER) – best lag (in months) where narrative scores lead sentiment.
- **`sentiment_predictions`**
  - `bank_name` (TEXT)
  - `year` (INTEGER)
  - `predicted_sentiment` (REAL) – forecast sentiment for a future year (e.g. 2026).
  - **PK**: (`bank_name`, `year`)
- **`narrative_highlights`**
  - `id` (INTEGER, PK AUTOINCREMENT)
  - `bank_name` (TEXT)
  - `year` (INTEGER)
  - `highlight` (TEXT) – short textual highlight for dashboards.

Helper functions in `db_cache.py`:

- `init_db()`: create tables.
- `register_bank`, `get_registered_banks`
- `get_cached_score`, `update_cache`
- `save_sentiment`, `get_sentiment`, `save_sentiment_score`
- `save_stock_return`, `get_stock_returns`
- `save_review_sentiment`
- `save_complaint_topics`
- `get_embedding`, `save_embedding`

These are consumed by transformation/market/dashboard scripts to avoid recomputation and to power BI-style dashboards.

---

## 4. Models and where they are used

### 4.1 Sentiment – text-only model

- **File**: `models/sentiment_model.py`
- **Underlying model**: Hugging Face Transformers
  - Pipeline: `"sentiment-analysis"`
  - Model: `"distilbert-base-uncased-finetuned-sst-2-english"`
- **Primary call site(s)**:
  - `scripts/processor.TextProcessor.process`
  - Potentially called in other analysis modules (root-cause, benchmarking, AI insights).
- **Purpose**:
  - Convert raw review text into label (`POSITIVE` / `NEGATIVE`) + confidence score.
  - Feeds into rating fusion (`sentiment_utils`) and then into `review_sentiments` / aggregated metrics.

### 4.2 Rating + text fusion

- **File**: `scripts/utils/sentiment_utils.py`
- **Key functions**:
  - `normalize_rating(rating)`: maps 1–5 rating → \[-1, +1].
  - `calculate_final_sentiment(text_sentiment, rating)`: combines text sentiment and normalized rating (70% text, 30% rating) with conflict-correction and clamping.
  - `sentiment_label(score)`: converts final score → `"Positive"`, `"Negative"`, `"Neutral"`.
  - `analyze_sentiment(text_sentiment, rating)`: full pipeline; returns `(final_score, label)`.
- **Usage**:
  - Called inside `TextProcessor.process` for each review.
  - The fused score is what is written into `review_sentiments.sentiment_score` and used for dashboards/analysis.

### 4.3 Embeddings – general text embeddings

- **File**: `models/embedding_model.py`
- **Underlying model**: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
- **Usage**:
  - `scripts/processor.TextProcessor`:
    - `encode(texts)` to obtain embeddings per review for clustering and topic/keyword analysis.
  - `scripts/db_cache.py` and `scripts/transformation_correlation.py`:
    - Separate **embedding cache** tables / helpers store arbitrary embeddings for PDF sentences and transformation analysis.
- **Purpose**:
  - Provide semantically meaningful vector representations of reviews (and sometimes PDF sentences) which can be clustered or similarity-searched.

### 4.4 Topic / cluster model

- **File**: `models/topic_model.py`
- **Underlying model**: `sklearn.cluster.KMeans` (`n_clusters=2`, `random_state=42`)
- **Usage**:
  - `scripts/processor.TextProcessor`:
    - `fit_predict(embeddings)` to assign each review to a topic cluster.
  - Additional topic-oriented scripts (e.g. `topic_discovery.py`) may build on this or extend it.
- **Purpose**:
  - Segment reviews into coherent groups (topics) for:
    - Cluster-specific sentiment averages.
    - Cluster volumes (% of reviews).
    - Cluster-level keyword extraction (`TfidfVectorizer`).

### 4.5 TF-IDF keyword model

- **File**: `scripts/processor.py`
- **Underlying model**: `sklearn.feature_extraction.text.TfidfVectorizer`
- **Usage**:
  - Per topic cluster:
    - Fit TF-IDF on cluster texts.
    - Compute mean TF-IDF scores.
    - Pick top terms as `"Key Themes"`.
- **Purpose**:
  - Extract interpretable keywords that describe each cluster for executives / dashboards.

### 4.6 Transformation / narrative embedding model

- **File**: `scripts/transformation_correlation.py`
- **Underlying model**: `SentenceTransformer("all-MiniLM-L6-v2")`
- **Usage**:
  - Compute embeddings for:
    - A curated list of `TRANSFORMATION_THEMES` (phrases about digital transformation, AI, etc.).
    - Sentences from PDF annual reports.
  - Cache embeddings in SQLite (`embedding_cache` table).
  - Compute similarity between PDF sentences and transformation themes to derive a per-year **transformation intensity score**.
- **Purpose**:
  - Quantify how strongly a bank’s corporate reports focus on digital/strategic transformation.
  - These scores are later correlated with:
    - Sentiment trends (`bank_trend_data.json`).
    - Stock returns.

### 4.7 Dashboard analytics models

Located mainly in `scripts/dashboard_data_engine.py`:

- **Topic sentiment aggregation**:
  - Uses static `TOPIC_KEYWORDS` mapping and keyword matching over `review_sentiments.review_text` to compute per-topic volumes and sentiment.
- **Correlation & lag models**:
  - Uses **Pearson correlation** (from `scipy.stats.pearsonr`) to link:
    - `narrative_scores.score` with `sentiment_scores.sentiment`.
  - Computes **lag** by testing correlations with 1–2 year shifts and stores the best lag (`narrative_lag.lag_months`).
- **Forecast / prediction model**:
  - Uses **`sklearn.linear_model.LinearRegression`** on historical `(year, sentiment)` pairs to forecast a future year’s sentiment (e.g. 2026).

All outputs are persisted back into:

- `narrative_sentiment_correlation`
- `narrative_lag`
- `sentiment_predictions`
- `narrative_highlights`

---

## 5. How to reason about the platform as ChatGPT

1. **Start from the pipeline**:
   - Use `pipeline_runner.py` to understand the sequence of computations and where outputs are written.
2. **Use `TextProcessor` as the central review engine**:
   - All review-based metrics ultimately come from `scripts/processor.TextProcessor.process`.
3. **Use `db_cache.py` + `dashboard_data_engine.py` to understand dashboard metrics**:
   - Tables define what is available to BI/UX; functions define how each metric is derived.
4. **Use `transformation_correlation.py` and `strategic_market_intelligence.py` for market/transformation logic**:
   - They explain how narrative intensity and sentiment relate to stock returns.
5. **Treat `models/` as the canonical place for ML components**:
   - Sentiment, embeddings, and topics are all wrapped under this folder and reused across scripts.

With just this file, you can reconstruct:

- What each pipeline stage does.
- What each DB table represents.
- Which models are called where.
- How to extend or debug the platform without reading every line of code.

