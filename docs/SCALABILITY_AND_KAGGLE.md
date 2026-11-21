# Scalability Evidence & Kaggle Dataset Status

**Purpose.** Reviewers asked why this project needs a Big Data stack when the shipped corpora are only 5.7 MB (6,000 synthetic full-text articles) and 10.7 MB (10,712 Reuters articles). This note records an actual larger-volume local run, the extrapolation to the Kaggle target volumes, and the real status of Kaggle credentials in this environment. All measured values are reproducible from `data/scale_benchmark.json`.

## 1. Proof-of-concept scale vs target production scale

| Corpus | Articles | Full-text CSV size | Status |
|---|---:|---:|---|
| Synthetic demo (`data/raw_news_articles.csv`) | 6,000 | ~5.7 MB | Shipped, used by all baseline runs |
| Reuters-21578 via NLTK (`data/real_news/raw_news_articles_real.csv`) | 10,712 | 10.7 MB (10,665,206 bytes) | Shipped, genuinely external corpus |
| **Benchmark corpus (`src/scale_benchmark.py`, default)** | **50,000** | **45.06 MiB (47,245,451 bytes)** | Generated locally, measured, then deleted |
| Kaggle *All the News* (143k edition) | 143,000 | Not downloaded | Target scale; adapter supports it |
| Kaggle *All the News 2.0* (2.7M edition) | 2,700,000 | Not downloaded | Target scale; adapter supports it |

The two shipped corpora are intentionally small **proof-of-concept** artifacts: they exercise the full PySpark → MapReduce → MongoDB → Parquet pipeline end to end and stay small enough for a repository/grading environment. The **target production scale** is the Kaggle archive, roughly 29×–270× the shipped Reuters corpus. The benchmark below demonstrates that the ingestion/cleaning/storage path was designed for that volume rather than for a spreadsheet.

## 2. What was actually measured

Command used (default parameters: `--rows 50000 --seed 42 --chunk-size 5000`):

```powershell
python src/scale_benchmark.py
```

`src/scale_benchmark.py` calls the existing generator in `src/generate_sample_data.py` once per 5,000-row chunk (derived seed = `42 + chunk_index`), then streams each chunk through pandas text hygiene (the same HTML/URL/entity stripping rules as `src/spark_pipeline.py` Stage 2) into Snappy-compressed Parquet partitions laid out as `year=YYYY/month=M/`. Peak in-memory frame size is one chunk, not the whole 50k corpus. The bulky CSV/Parquet files are deleted afterwards; only the JSON summary is kept. The canonical `data/raw_news_articles.csv` is never touched.

Measured on 2026-11-21 (Python 3.13.14, Windows 11, 12 logical CPUs, pandas 2.3.3, PyArrow 22.0.0, PySpark 4.2.0 available):

| Metric | Value (from `data/scale_benchmark.json`) |
|---|---:|
| Articles generated / retained | 50,000 / 50,000 |
| Generation time | 1.637 s |
| Cleaning + Parquet time | 3.310 s |
| Measured total (sum of stages) | 4.947 s |
| Uncompressed full-text CSV output | 45.06 MiB |
| Snappy Parquet output | 7.29 MiB (7,645,740 bytes) |
| `year`/`month` partitions written | 24 |
| Generation throughput | 30,540.21 rows/s (27.521 MB/s) |
| Cleaning + Parquet throughput | 15,108.00 rows/s (2.203 MB/s) |
| End-to-end throughput | **10,107.77 rows/s (1.474 MB/s)** |

Here `1 MB = 1,048,576 bytes (MiB)`, as stated in the JSON. Timings cover only the two measured stages; interpreter startup and JSON serialization are excluded.

## 3. Linear projection to Kaggle volumes — NOT a measurement

> **LINEAR PROJECTION (NOT A MEASUREMENT).** These numbers are arithmetic extrapolation of the measured 10,107.77 rows/s. They assume constant per-row cost and ignore I/O saturation, garbage collection, Spark shuffle/network, larger vocabularies, and storage growth. They are an order-of-magnitude argument, not a capacity plan.

| Target articles | Projected time (s) | Projected time (min) | Projected Parquet output |
|---:|---:|---:|---:|
| 143,000 | 14.15 | 0.24 | 20.85 MiB |
| 2,700,000 | 267.12 | 4.45 | 393.74 MiB |

Even with a pessimistic 10× slowdown once real corpus diversity, distributed LDA and cluster scheduling are included, a 2.7M-article workload remains a batch job measured in hours rather than a spreadsheet task. Conversely, the projected Parquet footprint (~394 MiB) is small enough that the bottleneck is CPU/NLP work and shuffle, not raw storage. The shipped ingestion adapter is chunked (`--chunk-size 100000`) precisely so the driver never needs to hold that corpus in memory.

## 4. Kaggle credentials: actual finding in this environment

Checked directly, with no download attempted:

| Check | Result |
|---|---|
| `KAGGLE_USERNAME` environment variable | **Not set** |
| `KAGGLE_KEY` environment variable | **Not set** |
| `import kagglehub` | **Unavailable** (`ModuleNotFoundError`) |
| `import kaggle` | **Unavailable** (`ModuleNotFoundError`) |

**Conclusion:** Kaggle API credentials and the Kaggle CLI/hub client are not available here, so no Kaggle dataset can be fetched automatically. No multi-GB dataset was downloaded. The pipeline remains ready for a user-supplied CSV, which is the documented offline path.

To use real Kaggle data, obtain the CSV yourself (Kaggle web UI, or configure `KAGGLE_USERNAME`/`KAGGLE_KEY` plus a Kaggle client on a machine where you install it) and run, from the project root:

```powershell
python src/ingest_kaggle_real.py --input <path> --chunk-size 100000
python run_all.py --force
```

The adapter streams the CSV in 100,000-row chunks, maps Kaggle column aliases (`headline` → `title`, `article` → `content`, etc.) into the canonical 12-column schema, purges short/empty articles, writes `data/raw_news_articles.csv`, and emits `data/date_imputation_report.json` for rows whose dates were imputed. Re-running the orchestrator with `--force` then rebuilds both the CSV-derived and PySpark stages instead of reusing cached artifacts (`--force-rerun-spark` remains available for re-running only the PySpark stage).

## 5. Why no Kaggle file is redistributed

Kaggle datasets are governed by the providers' licences and Kaggle's Terms of Service. The *All the News* archives are large, third-party news content and may not be/redistributed in a course repository without explicit permission; bulk redistribution would also be impractical (multi-GB artifacts, not version-control friendly). This project therefore ships only:

- the **synthetic** generator (`src/generate_sample_data.py`), which creates full-text, noisy articles locally with a fixed seed, and
- **Reuters-21578 via NLTK**, acquired through `src/ingest_real_news.py --source reuters` and documented in `data/real_news/provenance.json` (10,712 ingested articles, 90 true category labels, SHA-256 recorded).

Reuters-21578 is the genuinely external shipped corpus: real news wire text obtained from the NLTK data repository for research use, with provenance and deterministic date derivation recorded. It is real data, but it is still only ~10.7 MB — which is exactly why the volume benchmark in this note is needed to justify the Big Data architecture.

## 6. Reproducing and extending the evidence

```powershell
# Fast smoke test (~1 s): does not change the published 50k summary
python src/scale_benchmark.py --rows 5000

# Full default run (overwrites data/scale_benchmark.json with 50k figures)
python src/scale_benchmark.py

# Debug: keep the generated chunk CSVs and partitioned Parquet files
python src/scale_benchmark.py --keep-workdir

# Offline unit tests for the benchmark helpers (no corpus is generated)
python tests\run_tests.py
```

`--rows`, `--seed`, `--chunk-size`, `--work-dir` and `--output` are all configurable. The JSON records `generated_at`, parameters, stage timings, byte counts, throughput, labelled projections, environment, and caveats. `tests/test_scale_benchmark.py` validates the projection math, byte formatting, parameter validation and JSON schema keys without generating any corpus.
