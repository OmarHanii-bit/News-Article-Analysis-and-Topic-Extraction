# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses semantic versioning.

## [Unreleased]

### Added
- **Intelligence Command Center** (`src/build_command_center.py`,
  `dashboard/command_center.template.html`): a self-contained dark-mode
  dashboard with 11 topics, a brushable 24-month timeline, entity sentiment
  bars, a cross-filtered article feed and a slide-over inspector. Built from the
  committed PySpark summaries, HBase records and raw corpora.
- **Publish Dashboards** workflow (`.github/workflows/pages.yml`) and
  `docs/SHARING.md`: GitHub Pages is the primary public route; Streamlit
  Community Cloud, Docker and LAN options are documented.
- `requirements-dashboard.txt` — slim Streamlit Cloud runtime.
- `tests/test_command_center.py` — 6 payload/artifact contract tests.
- `CONTRIBUTING.md`, `CITATION.cff`, `.editorconfig` and a `docs/` index.
- README production-extensions summary and build/status badges.

### Changed
- Test suite is now **119 tests across 14 modules**; report, deck, README,
  CI labels and CONTRIBUTING were updated from the historical 113 count.
- Removed unused imports and the unused `ChannelEmpty` class to reduce noise.
- `PROJECT_REPORT.docx` now embeds 1400 px JPEGs instead of full-resolution
  PNGs: 9.9 MB → 1.8 MB with all 18 figures retained. Full-resolution charts
  remain in `visualizations/`.

### Fixed
- **LDA label alignment (2026-11-27):** LDA topic indices are not stable
  across runs, so the fixed index→label map could pair, for example, a
  Politics article with "Clinical Medicine & Genomics." Labels are now
  assigned **post-hoc from each cluster's majority `category`** (clustering
  remains unsupervised), and `metadata.label_diagnostics` records the
  majority and share; `metadata.label_mode` now records `majority-category`
  for the synthetic corpus instead of the obsolete `curated` value. The corrected five-cluster structure is Clinical
  Medicine & Genomics 951 (15.8%), Grid Decarbonization & Clean Energy 723
  (12.1%), Quantum Physics & Advanced Materials 470 (7.8%), Markets, Politics
  & Regulation 2,386 (39.8%; Business + Politics merged), and AI
  Infrastructure & Compute 1,470 (24.5%); one LDA component has no dominant
  assignments. Regenerated the Parquet lakehouse, analytics summary, drift
  monitor, forecast evaluation, MongoDB offline collections, 17 charts, all
  three HTML dashboards, report, DOCX and PPTX. The only negative-sentiment
  cluster is now Quantum (−0.028); the publisher cross-tab is reported as a
  null result (38.3–41.6% Markets share across every publisher), not as
  editorial bias.
- Refreshed report, README and deck metrics against the 2026-11-27 artifacts:
  pipeline 52.08 s, Reuters 68.4 s, Hadoop 4.28 s / 77 commands, HDFS
  6,236,201 logical bytes, Hive 26,594 rows scanned, overall sentiment +0.454,
  current publisher shares and MapReduce term frequencies.
- Documentation count drift: the Q&A bank is now consistently cited as 16
  answers (README, report, deck), and the test-suite description as 119 tests
  across 14 modules with the 118-test execution floor. Stale 12-answer,
  53-test and 7-module figures were removed.
- CI (pandas 3) crashed in `src/supervised_classifier.py`: `Series.values`
  returns an Arrow-backed array that scikit-learn cannot index, so
  `run_all.py` stopped before the MongoDB export. The split now materializes
  plain object arrays, and a regression check covers Arrow-backed strings.
- Streamlit **Intelligence Command Center** article cards read non-existent
  payload fields (`topic_label`, `timestamp`, `summary`), so every card showed
  "General" and "No summary available." Cards now use the payload's `topic`
  and `date` plus a new `brief_summary()` digest (markup and syndication
  boilerplate stripped, cut at a sentence boundary), and keyword-labelled rows
  no longer display a fabricated 95% LDA posterior.

### Removed
- `notebooks/News_Article_Analysis_Pipeline.html` — a derived 4.7 MB export of
  the notebook. Regenerate with `jupyter nbconvert --to html` when needed; the
  `.ipynb` with embedded outputs remains the source of truth.
- Hadoop staging/spool intermediates (`sqoop/articles_staging.csv`,
  `sqoop/articles_enriched.csv`, `pig/staging/`, `flume/spool/`) are now
  git-ignored. `run_all.py` rebuilds them; the committed `*_summary.json`
  files retain the measured record counts.
- `notebooks/News_Article_Analysis_Colab.ipynb` and its generator
  `scripts/create_colab_notebook.py` — an unmaintained duplicate of the
  executed pipeline notebook; nothing referenced either file.
- `~$OJECT_REPORT.docx` — a Microsoft Word lock file committed by accident.
- `data/hadoop_ecosystem/sqoop/publishers.csv` and `publishers.json` —
  generated table dumps; now git-ignored alongside the other Sqoop staging
  files.
- `data/hadoop_ecosystem/sqoop/news_staging.db` (2.0 MB) — the SQLite staging
  database is rebuilt by `run_all.py`; `sqoop_summary.json` keeps the measured
  path, table list and byte size. Now git-ignored with the other Sqoop
  intermediates.

## [2.1.0] - 2026-11-21

### Added
- **Supervised ground-truth evaluation** (`src/supervised_classifier.py`):
  9,549 Reuters-21578 articles, 17 editorial categories, 92.57% held-out test
  accuracy, 0.8423 macro-F1, 0.8515 5-fold CV macro-F1.
- **Live multi-publisher RSS ingestion** (`src/ingest_live_rss.py`): 272 articles
  at initial acquisition (the current corpus contains 289) from NYT, BBC, CBS and
  NPR with genuine RFC-822 timestamps.
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
