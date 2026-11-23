# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses semantic versioning.

## [Unreleased]

### Added
- `CONTRIBUTING.md`, `CITATION.cff`, `.editorconfig` and a `docs/` index.
- README production-extensions summary and build/status badges.

### Changed
- Removed the repeated "9 orchestrated ecosystem modules covering 10 named
  technologies" phrasing (19 occurrences) in favour of one canonical definition
  per document.
- `PROJECT_REPORT.docx` now embeds 1400 px JPEGs instead of full-resolution
  PNGs: 9.9 MB → 1.8 MB with all 18 figures retained. Full-resolution charts
  remain in `visualizations/`.

### Removed
- `notebooks/News_Article_Analysis_Pipeline.html` — a derived 4.7 MB export of
  the notebook. Regenerate with `jupyter nbconvert --to html` when needed; the
  `.ipynb` with embedded outputs remains the source of truth.

## [2.1.0] - 2026-11-21

### Added
- **Supervised ground-truth evaluation** (`src/supervised_classifier.py`):
  9,549 Reuters-21578 articles, 17 editorial categories, 92.57% held-out test
  accuracy, 0.8423 macro-F1, 0.8515 5-fold CV macro-F1.
- **Live multi-publisher RSS ingestion** (`src/ingest_live_rss.py`): 272 articles
  from NYT, BBC, CBS and NPR with genuine RFC-822 timestamps.
- **PySpark Structured Streaming** (`src/streaming_news_pipeline.py`): drop-folder
  micro-batches, VADER scoring, breaking-news alerts and a checkpointed Parquet
  sink.
- **Topic-drift monitoring** (`src/topic_drift_monitor.py`): Jensen-Shannon
  divergence and PSI over eight quarters.
- **Cluster IaC** (`docker-compose-cluster.yml`): 10-container NameNode,
  DataNodes, YARN, Spark and MongoDB topology.
- Three new visualizations and matching tests (113 tests across 13 modules).

### Changed
- Report Section 9 now separates *initial limitations* from *implemented
  extensions* and *remaining scope boundaries*.

## [2.0.0] - 2026-11-21

### Added
- Section 9 (limitations, threats to validity, future work) and a 14-entry
  reference list in `PROJECT_REPORT.md`.
- MIT `LICENSE`, `.env.example`, held-out forecast evaluation and a measured
  50,000-article scale benchmark.
- Committed raw datasets, `PROJECT_REPORT.docx` and both offline HTML dashboards
  so a fresh clone is self-contained.

### Fixed
- Report/deck claims now match artifacts (TechCrunch 41.7%, Bloomberg 11.9%,
  FT 13.9%; k-sweep knee k=3 vs production k=6; Reuters 10,788 available →
  10,712 ingested).
- Test runner guards on *executed* (non-skipped) tests instead of discovered.
- CI rebuilt the Parquet lakehouse that a committed summary had masked.

## [1.1.0] - 2026-11-21

### Added
- GitHub Actions workflow: compile, regenerate artifacts, run the suite.
- Kaggle streaming-ingestion adapter and its tests.

### Fixed
- Kaggle CSV column mapping and deprecated `datetime.utcnow()` usage.

## [1.0.0] - 2026-11-21

### Added
- Initial end-to-end pipeline: 6,000-article synthetic corpus, Hadoop MapReduce,
  PySpark LDA + VADER, Parquet lakehouse, MongoDB, Streamlit and offline
  dashboards, 17-slide deck and report.
