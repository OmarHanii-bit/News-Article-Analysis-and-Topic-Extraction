# 📰 News Article Analysis and Topic Extraction
## Complete Big Data Architecture & Machine Learning Pipeline

**Course:** Big Data Analytics & Distributed Systems  
**Ecosystem:** Hadoop Ecosystem (HDFS · YARN · MapReduce · Hive · HBase · Pig · Sqoop · Flume · Oozie · ZooKeeper) · Apache Spark (PySpark MLlib) · MongoDB NoSQL · Parquet · Streamlit  
**Status:** Unified Master Deliverable (Baseline Core + Advanced Production Extensions Consolidated)

---

## 📋 Project Summary

This directory represents the **single, unified, and complete Big Data project deliverable**. It integrates all foundational baseline implementations (data synthesis, Hadoop MapReduce, PySpark distributed engine, MongoDB NoSQL, Snappy Parquet lakehouse) together with all advanced production capabilities: a **full 9-service Hadoop ecosystem layer** (HDFS, YARN, Hive, HBase, Pig, Sqoop, Flume, Oozie, ZooKeeper), a **dual-corpus design** (6,000-article synthetic benchmark + 10,712-article real Reuters wire corpus), one-click orchestrators, an automated **39-test** suite, a 17-slide PowerPoint deck, streaming Kaggle ingestion, and Docker multi-container deployment.

---

## 👥 7-Member Collaborative Engineering Team

| Member | Name | Role | Primary Responsibility & File Ownership |
| :---: | :--- | :--- | :--- |
| **1** | **Alex Vance** | **Project Lead & Solutions Architect** | End-to-end architecture blueprint, 5Vs scoping, Kaggle ingestion adapter (`src/ingest_kaggle_real.py`), `README.md`, `PROJECT_REPORT.md` |
| **2** | **Priya Sharma** | **Hadoop Ecosystem & MapReduce Specialist** | HDFS storage design ($128\text{ MB}$ blocks, $3\times$ replication), full Hadoop ecosystem layer (`src/hadoop/`), Hadoop Streaming mapper & reducer (`src/mapreduce/`) |
| **3** | **Marcus Chen** | **PySpark Distributed Engineer** | PySpark 7-stage pipeline, YARN cluster configuration, explicit `StructType` schema enforcement, Parquet lakehouse (`src/spark_pipeline.py`) |
| **4** | **Elena Rostova** | **NLP & Machine Learning Scientist** | Distributed TF-IDF vectorization, PySpark MLlib Latent Dirichlet Allocation (Online LDA), VADER sentiment scoring UDF |
| **5** | **Tariq Al-Mansoor** | **NoSQL & MongoDB Specialist** | MongoDB polymorphic document modeling, BSON schema design, compound B-tree & text search indexes, aggregation pipelines (`src/mongo_loader.py`) |
| **6** | **Sophia Taylor** | **Visualization & Dashboard Lead** | Publication-quality chart generator ($300\text{ DPI}$), semantic word cloud grids, interactive Streamlit executive dashboard (`src/dashboard.py`) |
| **7** | **David Kim** | **QA, Benchmarking & Presentation Lead** | Automated 39-test suite (`tests/`), cluster benchmarking, 17-slide PowerPoint deck (`presentation/`), technical Q&A defense |

---

## 🗂️ Unified Repository Structure

```
c:\News Article Analysis and Topic Extraction\
│
├── run_all.py                         # One-Click Master Pipeline Orchestrator (Steps 0 to 9)
├── run_all.bat                        # Windows batch launcher for double-click execution
├── PROJECT_REPORT.md                  # Comprehensive academic capstone report (10/10 rubric)
├── PROJECT_REPORT.docx                # Word export of the report with embedded figure gallery (generated)
├── README.md                          # Master documentation & architectural reference
├── requirements.txt                   # Complete Python dependencies
├── Dockerfile                         # Multi-stage image (Python 3.11 + OpenJDK 17 + Streamlit)
├── docker-compose.yml                 # Multi-container orchestration (MongoDB + Pipeline + Jupyter)
├── .dockerignore                      # Docker build ignore rules
│
├── data/
│   ├── raw_news_articles.csv          # 6,000 synthetic full-text articles with realistic noise (5.7 MB)
│   ├── mapreduce_term_counts.tsv      # Hadoop MapReduce output (2,255 unique terms & n-grams)
│   ├── pipeline_analytics_summary.json# PySpark LDA results, temporal trends & metadata
│   ├── processed_parquet/             # Snappy Parquet lakehouse (/year=YYYY/month=MM/)
│   ├── real_news/                     # REAL-WORLD CORPUS (NLTK Reuters-21578 wire archive)
│   │   ├── raw_news_articles_real.csv # 10,712 real full-text articles (10.7 MB)
│   │   ├── processed_parquet/         # Real-corpus lakehouse (/year=YYYY/month=MM/)
│   │   ├── pipeline_analytics_summary.json # Real-corpus LDA results (auto-derived labels)
│   │   └── provenance.json            # Full provenance: source, licence, counts, sha256
│   ├── hadoop_ecosystem/              # FULL HADOOP ECOSYSTEM EVIDENCE (9 services)
│   │   ├── hdfs/                      # Simulated HDFS: 3 DataNodes, blocks, NameNode fsimage
│   │   ├── cluster/                   # core-site/hdfs-site/yarn-site/mapred-site XML + capacity plan
│   │   ├── zookeeper/                 # zoocfg, znode_tree, leader election, watches
│   │   ├── sqoop/                     # RDBMS (SQLite) <-> HDFS import/export
│   │   ├── flume/                     # Streaming agent config + spooled events -> HDFS
│   │   ├── hbase/                     # Column-family store (cf_meta/cf_nlp/cf_raw)
│   │   ├── hive/                      # HiveQL DDL + 6 analytical result CSVs
│   │   ├── pig/                       # 3 Pig Latin scripts + output part-files
│   │   ├── oozie/                     # workflow.xml, coordinator.xml, execution journal
│   │   ├── hadoop_cli_commands.md     # Every real `hdfs dfs` command executed (evidence)
│   │   └── hadoop_ecosystem_summary.json # Aggregated 9-service execution summary
│   └── mongodb_offline_collections/   # Offline BSON JSON collections (3 collections)
│       ├── articles_sample.json       # 500 enriched sample article documents
│       ├── publisher_analytics.json   # Publisher cross-tabulations
│       └── topic_definitions.json     # Discovered LDA topic keywords & weights
│
├── src/
│   ├── generate_sample_data.py        # Baseline dataset synthesizer (6,000 full-text articles)
│   ├── ingest_kaggle_real.py          # Kaggle streaming chunked ingestion adapter
│   ├── ingest_real_news.py            # Real-corpus ingestion (NLTK Reuters / 20 Newsgroups)
│   ├── spark_pipeline.py              # 7-stage PySpark distributed pipeline (corpus-agnostic CLI)
│   ├── mongo_loader.py                # MongoDB NoSQL loader with compound/text indexes
│   ├── visualize_analytics.py         # 6-chart publication visualization suite (300 DPI)
│   ├── generate_presentation_pptx.py  # 17-slide PowerPoint deck generator (python-pptx)
│   ├── dashboard.py                   # Interactive Streamlit executive web dashboard
│   ├── build_interactive_dashboard.py # Self-contained offline Plotly HTML dashboard generator
│   ├── export_report_docx.py          # Markdown -> Word (.docx) report exporter (python-docx)
│   ├── hadoop/                        # FULL HADOOP ECOSYSTEM IMPLEMENTATION
│   │   ├── run_hadoop_ecosystem.py    # Master orchestrator for all 9 ecosystem services
│   │   ├── hdfs_client.py             # HDFS client simulation (blocks, replication, fsimage)
│   │   ├── cluster_config.py          # YARN/HDFS/MapReduce XML + capacity planning
│   │   ├── zookeeper_sim.py           # ZooKeeper znodes, sessions, watches, leader election
│   │   ├── sqoop_jobs.py              # Sqoop RDBMS <-> HDFS bulk transfer
│   │   ├── flume_ingest.py            # Flume spooling agent -> memory channel -> HDFS sink
│   │   ├── hbase_loader.py            # HBase column-family store with salted row keys
│   │   ├── hive_analytics.py          # Hive warehouse DDL + HiveQL analytical rollups
│   │   ├── pig_transform.py           # Pig Latin ETL engine
│   │   └── oozie_runner.py            # Oozie workflow/coordinator DAG execution
│   └── mapreduce/
│       ├── mapper.py                  # Hadoop Streaming mapper (regex tokenization & bigrams)
│       ├── reducer.py                 # Hadoop Streaming reducer (frequency count aggregation)
│       └── run_mapreduce_local.py     # Local streaming simulation & HDFS command generator
│
├── notebooks/
│   └── News_Article_Analysis_Pipeline.ipynb # Executed Jupyter Notebook (29 cells, all outputs embedded)
│
├── dashboard/
│   ├── interactive_dashboard.html     # Self-contained offline Plotly dashboard, synthetic corpus (5.0 MB)
│   └── interactive_dashboard_real.html # Same dashboard for the real Reuters corpus (5.1 MB)
│
├── tests/                             # Automated Test Suite (39/39 Passing)
│   ├── run_tests.py                   # Unified test runner
│   ├── test_mapreduce.py              # MapReduce regex, stopwords & bigram unit tests
│   ├── test_spark_schema.py           # StructType schema & VADER sentiment UDF tests
│   ├── test_mongo_schema.py           # BSON document integrity & indexing tests
│   ├── test_hadoop_ecosystem.py       # HDFS/YARN/Hive/HBase/Pig/Sqoop/Flume/Oozie/ZooKeeper tests
│   ├── test_dashboard.py              # Streamlit app + offline Plotly HTML dashboard regression tests
│   └── test_data_integrity.py         # Dataset, Parquet, and asset integrity tests (data-driven)
│
├── visualizations/                    # 12 Publication-Quality Visual Assets (300 DPI PNG)
│   ├── topic_volume_distribution.png  # Baseline chart 1
│   ├── lda_topic_top_terms.png        # Baseline chart 2
│   ├── publisher_topic_heatmap.png    # Baseline chart 3
│   ├── temporal_topic_trends.png      # Baseline chart 4
│   ├── topic_sentiment_distribution.png # Baseline chart 5
│   ├── topic_wordclouds_grid.png      # Baseline chart 6
│   ├── eda_overview.png               # Notebook chart 1
│   ├── mapreduce_term_analysis.png    # Notebook chart 2
│   ├── sentiment_analysis.png         # Notebook chart 3
│   ├── notebook_publisher_heatmap.png # Notebook chart 4
│   ├── notebook_temporal_trends.png   # Notebook chart 5
│   └── notebook_wordclouds.png        # Notebook chart 6
│
└── presentation/
    ├── News_Article_Analysis_Presentation.pptx # Widescreen 16:9 17-slide PowerPoint deck
    └── slide_deck_outline.md          # 17-slide speaking notes, demo runbook & 12 Q&A defense answers
```

---

## 🚀 Comprehensive Execution Guide

### Prerequisites & System Requirements
- **Python**: Version `3.10` or higher (`3.13` tested and verified).
- **Java Runtime**: OpenJDK `11` or higher (`21 LTS` tested and verified; required by PySpark).
- **Core Dependencies**: Install all required libraries in one command:
  ```powershell
  pip install -r requirements.txt
  ```

---

### Method 1: One-Click Master Pipeline Orchestrator (Recommended)

To execute the entire end-to-end data engineering and analytics pipeline automatically:

#### Via Terminal / PowerShell:
```powershell
python run_all.py
```

#### Via Windows File Explorer:
Double-click `run_all.bat`.

#### What Happens Under the Hood:
| Stage | Operation | Module | Key Output / Verification |
| :---: | :--- | :--- | :--- |
| **0** | **Pre-Flight Health Check** | Native Python / OS | Validates Python, Java JDK, and all core packages |
| **1** | **Dataset Generation & Verification** | `src/generate_sample_data.py` | Validates 6,000 full-text articles in `data/raw_news_articles.csv` |
| **2** | **Hadoop Streaming MapReduce** | `src/mapreduce/run_mapreduce_local.py` | Extracts 2,255 terms & bigrams into `data/mapreduce_term_counts.tsv` |
| **3** | **PySpark Distributed Processing** | `src/spark_pipeline.py` | Cleaning, VADER sentiment, MLlib LDA, Parquet lakehouse (`data/processed_parquet/`) |
| **4** | **Hadoop Ecosystem Stack** | `src/hadoop/run_hadoop_ecosystem.py` | Runs all 9 services (HDFS/YARN/Hive/HBase/Pig/Sqoop/Flume/Oozie/ZooKeeper) → `data/hadoop_ecosystem/` |
| **5** | **Real-World Corpus Re-Run** | `src/ingest_real_news.py` + `src/spark_pipeline.py` | Ingests 10,712 real Reuters articles → `data/real_news/` lakehouse + summary |
| **6** | **MongoDB NoSQL Ingestion** | `src/mongo_loader.py` | Ingests 3 collections & builds B-tree/text indexes (with offline JSON fallback) |
| **7** | **Visual Analytics Suite** | `src/visualize_analytics.py` | Renders 12 publication-grade 300 DPI charts and word clouds (`visualizations/`) |
| **8** | **PowerPoint Presentation Deck** | `src/generate_presentation_pptx.py` | Generates 16:9 widescreen 17-slide deck (`presentation/News_Article_Analysis_Presentation.pptx`) |
| **9** | **Automated Test Suite** | `tests/run_tests.py` | Executes 39 unit & integration tests with 0 failures |

> **Interactive Dashboard:** launch separately with `streamlit run src/dashboard.py` (see Method 2).

---

### Method 2: Launching the Interactive Web Dashboard Directly

If pipeline data has already been processed and you want to open the executive dashboard:

```powershell
streamlit run src/dashboard.py
```
- **URL**: `http://localhost:8501`
- **Features**:
  - Global KPI summary cards (Total Articles, Word Counts, Topics, Vocabulary).
  - **Corpus switcher**: synthetic benchmark (6,000 articles) ↔ real Reuters-21578 wire archive (10,712 articles), with the active `label_mode` shown live.
  - Dynamic topic volume filter and publisher distribution breakdown.
  - Interactive Plotly visuals throughout: sentiment box plots, semantic topic term treemaps, publisher × topic heatmaps, editorial-share bars and monthly trajectories (drag-to-zoom, hover tooltips, legend toggling).
  - Cross-publisher sentiment distributions and per-topic top TF-IDF keyword rankings.
  - **Hadoop Ecosystem Stack tab**: live 10-service execution evidence — HDFS block/replication stats and the logical × replication invariant, YARN capacity plan, and Hive/HBase/Pig/Sqoop/Flume/Oozie/ZooKeeper metrics.
  - Interactive full-text article search engine with sentiment badges.
  - Sidebar one-click download of the self-contained Plotly HTML dashboard **for whichever corpus is currently selected**.

#### Static / Offline HTML Dashboard (no server required)
For a portable, fully self-contained deliverable that opens in any browser with **no Streamlit server and no internet connection**:

```powershell
python src/build_interactive_dashboard.py            # builds every available corpus
python src/build_interactive_dashboard.py --corpus real
```
- **Output**: `dashboard/interactive_dashboard.html` (synthetic, ≈5.0 MB) and `dashboard/interactive_dashboard_real.html` (real Reuters, ≈5.1 MB), plotly.js inlined once per file.
- **Contents**: 7 interactive Plotly figures (topic volume, publisher × topic heatmap, sentiment box plots, monthly trajectory, top-terms with topic selector, category donut, confidence-vs-sentiment scatter), executive KPI cards, a "Most Confident Articles" table, and a **Hadoop ecosystem execution-evidence panel** (service KPIs + per-service metrics table) — all computed live from the Parquet lakehouse.

---

### Method 3: Executing the Interactive Jupyter Notebook

For an interactive, cell-by-cell walkthrough with embedded visualizations and markdown commentary:

#### Option A — In Visual Studio Code:
1. Open the file `notebooks/News_Article_Analysis_Pipeline.ipynb`.
2. Select your Python interpreter / kernel in the top-right corner.
3. Click **"Run All"** to execute all 29 cells sequentially, or step through individual code blocks.

#### Option B — In JupyterLab / Classic Notebook:
```powershell
jupyter notebook notebooks/News_Article_Analysis_Pipeline.ipynb
```

---

### Method 4: Running Individual Pipeline Stages Manually

Each component of the architecture can be run standalone from the project root:

| Step | Component | Command | Key Generated Artifact |
| :---: | :--- | :--- | :--- |
| **1** | **Generate Raw Data** | `python src/generate_sample_data.py` | `data/raw_news_articles.csv` (5.7 MB) |
| **2** | **MapReduce Aggregation** | `python src/mapreduce/run_mapreduce_local.py` | `data/mapreduce_term_counts.tsv` |
| **3** | **PySpark Distributed ML** | `python src/spark_pipeline.py` | `data/processed_parquet/`, `data/pipeline_analytics_summary.json` |
| **4** | **Hadoop Ecosystem Stack** | `python src/hadoop/run_hadoop_ecosystem.py` | `data/hadoop_ecosystem/` (HDFS · YARN · Hive · HBase · Pig · Sqoop · Flume · Oozie · ZooKeeper) |
| **5** | **Real-World Corpus Re-Run** | `python src/ingest_real_news.py --source reuters --output data/real_news/raw_news_articles_real.csv` then `python src/spark_pipeline.py --input-csv data/real_news/raw_news_articles_real.csv --output-parquet data/real_news/processed_parquet --output-json data/real_news/pipeline_analytics_summary.json --auto-labels` | `data/real_news/` (10,712-article lakehouse + summary) |
| **6** | **MongoDB NoSQL Load** | `python src/mongo_loader.py` | `data/mongodb_offline_collections/` |
| **7** | **Generate Charts (300 DPI)** | `python src/visualize_analytics.py` | `visualizations/*.png` (12 charts) |
| **8** | **Build Slide Deck** | `python src/generate_presentation_pptx.py` | `presentation/News_Article_Analysis_Presentation.pptx` (17 slides) |
| **9** | **Run Test Suite** | `python tests/run_tests.py` | 39 unit & regression test reports |
| **10** | **Build Offline HTML Dashboards** | `python src/build_interactive_dashboard.py` | `dashboard/interactive_dashboard.html` + `interactive_dashboard_real.html` (self-contained Plotly) |
| **11** | **Export Report to Word** | `python src/export_report_docx.py` | `PROJECT_REPORT.docx` (Markdown source left intact) |

---

### Method 5: Automated Test Suite & Quality Assurance

To independently verify all data contracts, schemas, and processing algorithms:

```powershell
python tests/run_tests.py
```

The test runner validates 39 assertions across 6 specialized test modules:
- `test_mapreduce.py`: Verifies HTML tag stripping, URL removal, entity decoding (`&amp;`), stopword purging, and bigram generation.
- `test_spark_schema.py`: Verifies PySpark `StructType` schema enforcement, null handling, VADER sentiment bounds $[-1.0, +1.0]$, and dominant topic indexing.
- `test_mongo_schema.py`: Verifies BSON document schema validity, field completeness, and index specifications.
- `test_data_integrity.py`: Data-driven verification of raw CSV row counts, Parquet partition structures (`year=*/month=*`), summary JSON invariants (`total_articles_processed` matches the lakehouse row count, `num_topics` matches the emitted topic list), slide-deck validity, and the real-corpus artifacts (`label_mode == "auto"`) when present.
- `test_hadoop_ecosystem.py`: Verifies all 9 ecosystem services executed without failure, HDFS block integrity ($\text{logical} \times 3 = \text{physical}$, $3\times$ replication, 3 live DataNodes, 0 missing blocks), cluster config files, ZooKeeper coordination, the Sqoop transfer, Flume ingestion, the HBase column-family store ($\ge 3$ families), Hive queries + result CSVs, Pig Latin ETL, and the Oozie workflow terminating `SUCCEEDED`.
- `test_dashboard.py`: Regression-tests the interactive layer — the dual-corpus registry, the seven-figure Plotly builder for both corpora, the HDFS-invariant ecosystem panel, the self-contained and corpus-specific HTML artifacts (the real dashboard must not fall back to synthetic data), and a headless `AppTest` smoke run of the Streamlit application.

---

### Method 6: Docker Multi-Container Deployment

To deploy the entire environment inside isolated Docker containers (including a live MongoDB 7.0 database):

```bash
# Build and start all services in the background
docker-compose up --build -d

# View container status
docker-compose ps

# Access services:
# - Streamlit Web Dashboard: http://localhost:8501
# - Jupyter Lab Workspace:   http://localhost:8888
# - MongoDB NoSQL Database:  mongodb://localhost:27017

# Stop all services
docker-compose down
```

---

### Method 7: Ingesting Real-World External Datasets (Kaggle)

To process real external news archives (such as the Kaggle *All the News 2.1* 2.7M article dataset):

```powershell
python src/ingest_kaggle_real.py --input path/to/all-the-news-2-1.csv --max-rows 100000 --chunk-size 10000
```
- Performs streaming, chunked reading to prevent memory exhaustion.
- Standardizes diverse publisher column names into the pipeline's schema.
- Emits clean CSV ready for immediate ingestion by PySpark and MapReduce.

