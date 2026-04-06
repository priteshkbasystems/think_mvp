## New components and features (extensions)

This section documents additional modules and tables added after the original README so that GPT has an up‑to‑date view of the platform.

### New scripts and pipeline steps

- `scripts/financial_extraction.py` / `scripts/financial_extraction_pipeline.py` – extract revenue, net profit, operating income, total assets, ROE from PDFs already indexed in `pdf_cache`, and store per‑bank/year rows in `financial_metrics`. Integrated as STEP 28 in `pipeline_runner.py`.
- `scripts/corporate_sentiment_model.py` – runs document‑level corporate sentiment using `SentimentModel` over sentences from each PDF in `pdf_cache`, writing `(bank_name, year, sentiment)` into `corporate_sentiment`.
- `scripts/corporate_topic_pipeline.py` – computes topic‑level corporate narrative scores from corporate PDFs and writes them via `save_corporate_topic_sentiment` into `corporate_topic_sentiment`. Used as STEP 4 – CORPORATE TOPIC SENTIMENT.
- `scripts/topic_alignment.py` – aligns corporate topics (`corporate_topic_sentiment`) with customer complaint topics (`complaint_topics`) using SentenceTransformer embeddings; invoked in STEP 6 – TOPIC ALIGNMENT.
- `scripts/aspect_sentiment.py` – computes aspect‑level sentiment (mobile app, customer service, login, payments, security, pricing, UX) by combining text sentiment and rating; used in STEP 7 – ASPECT SENTIMENT.
- `scripts/scenario_simulator.py` – provides a simple simulator that learns a mapping from narrative scores to sentiment scores and exposes a `predict(new_narrative_score)` function; used in STEP 11 – SCENARIO SIMULATION.
- `scripts/parallel_executor.py` – thin wrapper over `ThreadPoolExecutor` to parallelise per‑document operations (e.g. corporate sentiment analysis).
- `scripts/pipeline_manager.py` – manages a `pipeline_runs` table so each pipeline step knows whether it needs to rerun or can be skipped as cached.
- `scripts/pipeline_dependency_manager.py` – encodes dependencies between steps and exposes `dependency_changed(step)` so downstream steps rerun when upstream ones have not succeeded.

### New / extended database tables

- `financial_metrics` – per‑bank, per‑year financial KPIs (revenue, net_profit, operating_income, total_assets, roe) extracted from corporate PDFs.
- `corporate_sentiment` – per‑bank, per‑year sentiment score for corporate documents.
- `corporate_topic_sentiment` – per‑bank, per‑year, per‑topic corporate narrative strength (topic names and scores).
- `pipeline_runs` – records `step_name`, `status`, and `last_run` to support restartable, dependency‑aware pipeline execution.


### End‑to‑end pipeline steps (`pipeline_runner.py`)

Orchestration is in `pipeline_runner.py` (authoritative). Active steps (in order): **1** data indexing, **2** sentiment trend, **4** corporate topic sentiment, **5** narrative scores, **6** topic alignment, **7** aspect sentiment, **8** dashboard data engine, **11** scenario simulation, **12** transformation impact score, **13** source concordance, **15** topic sentiment correlation, **16** journey sentiment, **19** transformation competencies, **20** transformation performance index, **23** corporate sentiment model, **24** success factor detection, **26** transformation lag, **27** topic mapping, **28** financial extraction.

### Supporting analysis modules (used by the pipeline)

- `scripts/source_concordance.py` / `scripts/source_concordance_pipeline.py` – concordance across review sources.
- `scripts/topic_sentiment_correlation.py` / `scripts/topic_sentiment_correlation_pipeline.py` – topic‑level correlation pipeline.
- `scripts/transformation_impact_score.py` – STEP 12.
- `scripts/transformation_performance_index.py` – STEP 20.
- `scripts/transformation_lag_analysis.py` / `transformation_lag_pipeline.py` – lag analysis.
- `scripts/journey_sentiment.py` / `scripts/journey_sentiment_pipeline.py` – STEP 16.
- `scripts/corporate_topic_sentiment.py` – used by `corporate_topic_pipeline`.
- `scripts/transformation_competency_engine.py` – STEP 19.
- `scripts/success_factor_detection.py` / `scripts/success_factor_pipeline.py` – STEP 24.
- `scripts/corporate_sentiment_analyzer.py` / `scripts/corporate_sentiment_pipeline.py` – STEP 23.
- `scripts/topic_mapping_engine.py` – STEP 27.
- `scripts/progress_tracker.py` – `step_progress` checkpoints.
- `scripts/financial_extraction.py` / `financial_extraction_pipeline.py` – STEP 28.

## Overview

This repository implements an **AI Banking Intelligence Platform** that:
- Ingests **bank customer reviews**, **corporate PDFs (annual reports, presentations)**, and **stock price series**.
- Runs **sentiment + rating fusion**, **topic/root-cause analysis**, and **transformation/narrative scoring**.
- Stores results in a **SQLite database** and writes multiple **text/JSON reports**.

The main orchestration entrypoint is `pipeline_runner.py`. Individual experiments can be run via `main.py`, `trend_analysis.py`, and the scripts under `scripts/`.

---

## Folder structure (high level)

- `pipeline_runner.py`  
  Full end‑to‑end pipeline runner; see file for the active step list.

- `main.py`  
  Simple one‑bank review analysis using `TextProcessor` (older, less central once the full pipeline is used).

- `trend_analysis.py`  
  Yearly sentiment trend engine over multiple banks’ review Excel files.

- `benchmark.py`  
  Benchmarking / console report that uses `TextProcessor` and `RootCauseAnalyzer` together for a single bank dataset.

- `models/`  
  - `sentiment_model.py` – Hugging Face sentiment model wrapper.  
  - `embedding_model.py` – SentenceTransformer wrapper for review embeddings.  
  - `topic_model.py` – KMeans clustering wrapper for topic IDs.

- `scripts/` (core business logic)
  - `processor.py` – Central review processing engine (sentiment+rating fusion, clustering, keyword extraction, executive summary, benchmark metrics).
  - `root_cause_analyzer.py` – Negative review root‑cause classifier and emerging complaint topic discovery (writes complaint topics to DB).
  - `db_cache.py` – Central SQLite DB schema and helper functions (all tables and most DB writes/reads live here).
  - `data_indexer.py` – Discovers banks and indexes stock price files into yearly returns in DB.
  - `dashboard_data_engine.py` – Populates dashboard‑oriented tables in DB from other tables (complaint topics, narrative vs sentiment correlation, lag, predictions, highlights).
  - `narrative_score_generator.py` – Converts `pdf_cache` scores into per‑bank narrative scores table.
  - `topic_discovery.py` – BERTopic‑based topic discovery module for negative complaints.
  - `utils/sentiment_utils.py` – Sentiment+rating fusion utilities and label mapping.

---

## Database structure

All persistent analytics are stored in a single SQLite file:
- **Path**: `/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db`

The canonical schema is created in `scripts/db_cache.py:init_db()` and partially duplicated in some other scripts.

### Core tables

- **`pdf_cache`**  
  - `file_path` (TEXT, PK) – Absolute path to a corporate PDF.  
  - `last_modified` (REAL) – File modification timestamp.  
  - `year` (INTEGER) – Year inferred from filename/path.  
  - `score` (REAL) – Transformation / narrative intensity score for that document.  
  - **Purpose**: Cache of expensive PDF → transformation score computations (e.g. for `narrative_score_generator` and corporate pipelines that populate scores).

- **`banks`**  
  - `bank_name` (TEXT, PK) – Display name of the bank.  
  - **Purpose**: List of all banks discovered by `data_indexer.py` (`register_bank`).

- **`sentiment_scores`**  
  - `bank_name` (TEXT)  
  - `year` (INTEGER)  
  - `sentiment` (REAL) – Aggregated sentiment score (often from `trend_analysis` or transformation pipeline).  
  - `contradiction_ratio` (REAL) – Optional metric for rating vs text contradictions.  
  - PK `(bank_name, year)`.  
  - **Writes**: `db_cache.save_sentiment`, transformation engines.  
  - **Reads**: `dashboard_data_engine.compute_correlation`, `.compute_lag`, `.generate_prediction`, strategic/AI insight scripts.

- **`stock_returns`**  
  - `bank_name` (TEXT)  
  - `year` (INTEGER)  
  - `return` (REAL) – Yearly stock return.  
  - PK `(bank_name, year)`.  
  - **Writes**: `data_indexer.save_stock_return` (called from `data_indexer.index_stock_data`).  
  - **Reads**: any analytics that join sentiment with market data via SQL.

- **`review_sentiments`**  
  - `id` (INTEGER, PK AUTOINCREMENT)  
  - `bank_name` (TEXT)  
  - `year` (INTEGER)  
  - `review_text` (TEXT)  
  - `rating` (REAL)  
  - `sentiment_score` (REAL) – Final fused sentiment score.  
  - `sentiment_label` (TEXT) – Positive/Neutral/Negative.  
  - **Purpose**: Per‑review sentiment (used for topic sentiment and dashboard metrics).  
  - **Writes**: functions in `db_cache.py` and any review ingestion pipeline using `TextProcessor`.  
  - **Reads**: `dashboard_data_engine.generate_topic_sentiment`.

- **`complaint_topics`**  
  - `id` (INTEGER, PK AUTOINCREMENT)  
  - `bank_name` (TEXT)  
  - `topic_id` (INTEGER or TEXT label)  
  - `keywords` (TEXT) – Comma‑separated keywords.  
  - `review_count` (INTEGER)  
  - `created_at` (TIMESTAMP, default now)  
  - **Writes**:
    - `db_cache.save_complaint_topics` (called by `RootCauseAnalyzer.analyze` using BERTopic engine).  
    - `dashboard_data_engine.generate_topic_sentiment` (keyword‑based complaint counts).  

- **`embedding_cache`**  
  - `id` (INTEGER, PK AUTOINCREMENT) – in `db_cache` version.  
  - `text_hash` (TEXT, UNIQUE) – MD5 hash of text.  
  - `embedding` (BLOB) – Serialized float32 vector.  
  - `created_at` (TIMESTAMP)  
  - **Purpose**: Cache SentenceTransformer embeddings of corporate text to avoid recompute.  
  - **Writes**: `db_cache.save_embedding`.  
  - **Reads**: `db_cache.get_embedding`.

- **`narrative_scores`**  
  - `bank_name` (TEXT)  
  - `year` (INTEGER)  
  - `score` (REAL) – Narrative score per bank/year, typically `round(pdf_cache.score * 100)`.  
  - PK `(bank_name, year)`.  
  - **Writes**: `narrative_score_generator.generate_narrative_scores`.  
  - **Reads**: `dashboard_data_engine.compute_correlation`, `.compute_lag`.

- **`narrative_sentiment_correlation`**  
  - `bank_name` (TEXT, PK)  
  - `correlation` (REAL) – Pearson correlation between narrative score and sentiment over years.  
  - **Writes**: `dashboard_data_engine.compute_correlation`.  

- **`narrative_lag`**  
  - `bank_name` (TEXT, PK)  
  - `lag_months` (INTEGER) – Best lag (in months) where narrative leads sentiment.  
  - **Writes**: `dashboard_data_engine.compute_lag`.

- **`sentiment_predictions`**  
  - `bank_name` (TEXT)  
  - `year` (INTEGER) – Prediction target year (e.g. 2026).  
  - `predicted_sentiment` (REAL) – Linear regression prediction.  
  - PK `(bank_name, year)`.  
  - **Writes**: `dashboard_data_engine.generate_prediction`.

- **`narrative_highlights`**  
  - `id` (INTEGER, PK AUTOINCREMENT)  
  - `bank_name` (TEXT)  
  - `year` (INTEGER)  
  - `highlight` (TEXT) – Simple curated highlight string per document/year.  
  - **Writes**: `dashboard_data_engine.generate_highlights`.

---

## Models used (file → model → purpose)

- **`models/sentiment_model.py`**  
  - Model: Hugging Face pipeline `"distilbert-base-uncased-finetuned-sst-2-english"`.  
  - API: `SentimentModel.predict_batch(texts)` → list of `{label, score}`.  
  - Use: Raw text sentiment for each review; fed into rating fusion in `TextProcessor`.

- **`models/embedding_model.py`**  
  - Model: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`.  
  - API: `EmbeddingModel.encode(texts)` → tensor of embeddings.  
  - Use: Cluster customer reviews into topics via KMeans (`TopicModel`).

- **`models/topic_model.py`**  
  - Model: `sklearn.cluster.KMeans` (`n_clusters=2`).  
  - API: `TopicModel.fit_predict(embeddings)` → cluster IDs.  
  - Use: Segment reviews into coarse topics for cluster‑level sentiment and keyword extraction.

- **`scripts/utils/sentiment_utils.py`**  
  - No external model; implements rating+sentiment fusion logic.  
  - `normalize_rating`, `calculate_final_sentiment`, `sentiment_label`, `analyze_sentiment`.  
  - Use: Combine text sentiment from model with 1–5 rating, correct contradictions, clamp final score, map to Positive/Neutral/Negative.

- **`scripts/topic_discovery.py`**  
  - Model: `BERTopic` with `SentenceTransformer("all-MiniLM-L6-v2")`.  
  - Use: Discover fine‑grained emerging complaint topics from negative reviews.

- **`scripts/root_cause_analyzer.py`**  
  - Model: `SentenceTransformer("all-MiniLM-L6-v2")`.  
  - Use: Map negative reviews to one of several predefined root‑cause themes via cosine similarity (Performance, App Crashes, Login, etc.).

---

## Main pipeline flow (step by step)

### 1. `pipeline_runner.py`

**Purpose**: High‑level orchestrator. Each step imports and calls a `main()`/function from another module; failures abort the run.

Steps (see `pipeline_runner.py` for imports and order): data indexing, sentiment trend, corporate topic sentiment, narrative scores, topic alignment, aspect sentiment, dashboard engine, scenario simulation, transformation impact, source concordance, topic/journey sentiment, transformation competencies and performance index, corporate sentiment model, success factors, transformation lag, topic mapping, financial extraction.

---

## Per‑file function and query summary

### `models/sentiment_model.py`

- **`class SentimentModel`**  
  - **`__init__`** – Instantiates Hugging Face pipeline for SST‑2 sentiment.  
  - **`predict_batch(texts)`** – Returns list of dicts `{label, score}` per input text.

### `models/embedding_model.py`

- **`class EmbeddingModel`**  
  - **`__init__`** – Chooses `cuda` or `cpu`; loads `"paraphrase-multilingual-MiniLM-L12-v2"`.  
  - **`encode(texts)`** – Returns embeddings tensor (used for clustering).

### `models/topic_model.py`

- **`class TopicModel`**  
  - **`__init__(n_clusters=2)`** – Creates `KMeans` model.  
  - **`fit_predict(embeddings)`** – Converts tensor to numpy and returns cluster IDs.

### `scripts/utils/sentiment_utils.py`

- **`normalize_rating(rating)`** – Map 1–5 rating to \[-1, +1\].  
- **`calculate_final_sentiment(text_sentiment, rating)`** – Fuse text sentiment & normalized rating, apply conflict corrections and clamp.  
- **`sentiment_label(score)`** – Map final score to `"Positive"`, `"Neutral"`, `"Negative"`.  
- **`analyze_sentiment(text_sentiment, rating)`** – Convenience wrapper returning `(final_score, label)`.

### `scripts/processor.py`

- **`class TextProcessor`**  
  - **`__init__`** – Creates `SentimentModel`, `EmbeddingModel`, `TopicModel(n_clusters=2)`.  
  - **`process(texts, ratings=None)`**  
    - If `ratings` absent, defaults to all 3 (neutral).  
    - Calls `SentimentModel.predict_batch` and `EmbeddingModel.encode`.  
    - Applies safe clustering (all cluster 0 if too few samples).  
    - For each review:  
      - Builds signed sentiment from model output,  
      - Fuses with rating via `analyze_sentiment`,  
      - Records `final_score`, label, rating, and topic cluster in `results`.  
    - Aggregates per‑cluster scores and texts; computes `overall_sentiment`.  
    - Extracts top TF‑IDF keywords per cluster when enough texts.  
    - Builds an executive summary text (clusters, key themes, strategic insight).  
    - Returns `(results, executive_summary, benchmark_data)` where `benchmark_data["overall_sentiment"]` is used by `trend_analysis` and downstream correlation.

### `scripts/root_cause_analyzer.py`

- **`class RootCauseAnalyzer`**  
  - **`__init__`** – Loads `SentenceTransformer("all-MiniLM-L6-v2")`, configures fixed root‑cause themes and embeddings, creates `ComplaintTopicDiscovery` instance.  
  - **`_extract_score(sentiment)`** – Normalizes heterogeneous sentiment inputs (dicts, tensors, floats) to a single float \[-1,+1\].  
  - **`classify_root_cause(text)`** – Embeds text, compares with theme embeddings, returns best theme or `"Other Complaints"` if low similarity.  
  - **`analyze(texts, sentiments, bank_name=None, save_to_file=False, output_dir=None, verbose=True)`**  
    - Counts Positive/Neutral/Negative reviews using `sentiment_label`.  
    - For negative reviews: classifies root‑cause, accumulates counts.  
    - If enough negative texts: calls `ComplaintTopicDiscovery.discover_topics`, then `save_complaint_topics(bank_name, topics)` (DB write into `complaint_topics`).  
    - Constructs a root‑cause analysis report string.  
    - Optionally saves that report to a timestamped text file.  
    - Returns `Counter` of root causes.

### `scripts/topic_discovery.py`

- **`class ComplaintTopicDiscovery`**  
  - **`__init__`** – Loads SentenceTransformer model and initializes BERTopic with that embedding model.  
  - **`discover_topics(texts)`** – Runs BERTopic clustering, collects top 5 keywords per non‑outlier topic, returns `{topic_id: [keywords...]}`.

### `scripts/db_cache.py` (DB helper + schema)

- **`init_db()`** – Connects to DB (creates if missing) and issues `CREATE TABLE IF NOT EXISTS` statements for all core tables listed above, then commits.  
- **`get_file_modified_time(path)`** – Wraps `os.path.getmtime`.  
- **`get_cached_score(file_path)`** – `SELECT last_modified, year, score FROM pdf_cache WHERE file_path=?`.  
- **`update_cache(file_path, last_modified, year, score)`** – `INSERT OR REPLACE INTO pdf_cache (...) VALUES (?,?,?,?)`.  
- **`register_bank(bank_name)`** – `INSERT OR IGNORE INTO banks (bank_name)`.  
- **`get_registered_banks()`** – `SELECT bank_name FROM banks`.  
- **`save_sentiment(bank_name, year, sentiment, contradiction_ratio)`** – `INSERT OR REPLACE INTO sentiment_scores`.  
- **`get_sentiment(bank_name)`** – Reads all sentiment rows per bank (full function body not shown but used where needed).  
- **`save_review_sentiment(...)`** – Inserts per‑review entries into `review_sentiments` (present in the omitted tail).  
- **`save_complaint_topics(bank_name, topics)`** – For each `(topic_id, keywords)` pair, `INSERT INTO complaint_topics`.  
- **`get_embedding(text)`** – `SELECT embedding FROM embedding_cache WHERE text_hash=?` (hash by MD5).  
- **`save_embedding(text, embedding)`** – `INSERT OR IGNORE INTO embedding_cache (text_hash, embedding) VALUES (?, ?)`.  
- **Other helpers** – Additional getters/writers around `narrative_scores`, `narrative_sentiment_correlation`, `narrative_lag`, `sentiment_predictions`, `narrative_highlights` as needed by the pipeline.

### `scripts/data_indexer.py`

- **Constants**  
  - `BASE_CORP_PATH` – Root folder of all bank corporate documents.  
- **`discover_banks(base_path)`**  
  - Scans bank folders under `base_path`.  
  - For each bank: finds `stock_price` subfolder and its CSV/XLSX file.  
  - Returns dict `{display_name: {"folder": ..., "stock": path_or_None}}`.  
- **`load_stock_dataframe(file_path)`**  
  - Loads an Excel sheet with `Date` and `Price` columns, or a CSV with fallback encodings.  
  - Returns pandas DataFrame or `None`.  
- **`compute_yearly_returns(file_path)`**  
  - Cleans and parses `Date`/`Price` columns, groups by year, returns yearly percentage return dict.  
- **`index_stock_data(bank_name, stock_file)`**  
  - Calls `compute_yearly_returns`, then for each year calls `save_stock_return(bank_name, year, value)` (DB write into `stock_returns`).  
- **`main()`**  
  - Calls `init_db()`.  
  - Runs `discover_banks(BASE_CORP_PATH)`.  
  - For each discovered bank, calls `register_bank` and `index_stock_data`.  
  - This step is invoked by `pipeline_runner`.

### `trend_analysis.py`

- **Config**  
  - `BANK_PATHS` – Map of bank name → reviews folder path.  
  - `OUTPUT_PATH` – Text report path.  
- **`load_reviews_with_dates(folder_path)`**  
  - Iterates `.xlsx` files in a folder, expects `Date` and `review` columns.  
  - Cleans dates, drops empty reviews.  
  - Returns list of `{year, text}` dicts.  
- **`detect_trend(year_sentiments)`**  
  - Compares first vs last year sentiment; returns `"Improving"`, `"Declining"`, or `"Stable"` (or `"Insufficient Data"`).  
- **`main()`**  
  - For each bank in `BANK_PATHS`:
    - Loads reviews with `load_reviews_with_dates`.  
    - Groups texts by year.  
    - For each year: calls `TextProcessor.process(texts)` to get `benchmark_data["overall_sentiment"]`.  
    - Stores yearly sentiment series and prints them.  
    - Calls `detect_trend` and writes a simple bank trend text report.  
  - Output: `bank_trend_report.txt` and (via related scripts) `bank_trend_data.json`.

### `scripts/dashboard_data_engine.py`

- Constants: `DB_PATH`, `TOPIC_KEYWORDS`.  
- **`generate_topic_sentiment(cursor)`**  
  - **Read**: `SELECT bank_name, review_text, sentiment_score FROM review_sentiments`.  
  - For each review, checks which `TOPIC_KEYWORDS` category its text mentions.  
  - Aggregates count and sentiment per `(bank, topic)` and **writes**:
    - `INSERT INTO complaint_topics (bank_name, topic_id, keywords, review_count) VALUES (...)`.  
- **`compute_correlation(cursor)`**  
  - **Read**: For each bank in `narrative_scores`, selects `year, score` and `year, sentiment` from `sentiment_scores`.  
  - Computes Pearson correlation between narrative and sentiment per bank.  
  - **Write**: `INSERT OR REPLACE INTO narrative_sentiment_correlation (bank_name, correlation)`.  
- **`compute_lag(cursor)`**  
  - For each bank, tries lags of 1–2 years between narrative and future sentiment.  
  - Stores best lag (in months) in `narrative_lag`.  
- **`generate_prediction(cursor)`**  
  - For each bank in `sentiment_scores`, fits linear regression `year → sentiment`.  
  - Predicts sentiment for year 2026.  
  - **Write**: `INSERT OR REPLACE INTO sentiment_predictions`.  
- **`generate_highlights(cursor)`**  
  - **Read**: `SELECT file_path, year FROM pdf_cache`.  
  - Derives bank name from path, creates simple highlight strings.  
  - **Write**: `INSERT INTO narrative_highlights`.  
- **`main()`**  
  - Opens DB; sequentially calls all generator functions then commits.

### `scripts/narrative_score_generator.py`

- **`generate_narrative_scores()`**  
  - **Read**: `SELECT file_path, year, score FROM pdf_cache`.  
  - Converts `file_path` to `bank_name` (folder naming convention).  
  - Computes integer `narrative_score = round(score * 100)`.  
  - **Write**: `INSERT OR REPLACE INTO narrative_scores (bank_name, year, score)`.  

### `main.py`

- Single‑bank CLI driver:
  - Validates `DATA_PATH` for reviews.  
  - Recursively loads `.xlsx` files; infers a review text column from several possible names.  
  - Appends all review texts to `all_texts`.  
  - Runs `TextProcessor.process(all_texts)` (older signature; in newer version also supports ratings if passed).  
  - Saves detailed results JSON and executive report TXT under `OUTPUT_PATH`/`REPORT_PATH`.  
  - Prints a few sample results.

### `benchmark.py`

- Combines `TextProcessor` and `RootCauseAnalyzer` for performance/coverage checks.  
- Typical flow: load reviews, run processor for sentiment, then root‑cause to see negative complaint categories.  
- Does not write to DB; primarily console output and demo.

---

## How to extend this platform with ChatGPT

Given this README, a ChatGPT agent can:
- Locate high‑level pipeline entry (`pipeline_runner.py`) and run or modify individual stages.  
- Understand where to plug in new models (e.g. alternate sentiment or embedding models) by editing `models/*.py`.  
- Add new DB‑level metrics by extending `db_cache.py` (schema + helpers) and then referencing them from `dashboard_data_engine.py`.  
- Introduce new report types by reading from existing tables (`sentiment_scores`, `review_sentiments`, `narrative_scores`, `stock_returns`) and writing text/JSON outputs in a new script.  
- Safely modify only the relevant step (e.g. sentiment fusion, root‑cause themes, transformation themes) without breaking the orchestration contract used by `pipeline_runner.py`.

