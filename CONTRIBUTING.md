# Contributing

Thanks for your interest in the News Article Analysis & Topic Extraction pipeline.
This document explains how to set up, verify and extend the project without
breaking the reproducibility guarantees the report relies on.

## 1. Set up

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
python -m nltk.downloader -q vader_lexicon reuters punkt punkt_tab stopwords
```

Java 17 is required for PySpark (`actions/setup-java` installs Temurin in CI).

## 2. Generate artifacts before testing

The integration tests read generated artifacts that are intentionally **not**
committed (Parquet lakehouses, MongoDB offline collections, simulated HDFS
blocks). Materialize them first:

```bash
python run_all.py            # full pipeline (idempotent; skips existing stages)
python run_all.py --force    # force full regeneration
python tests/run_tests.py    # 119 tests; exits non-zero on any failure
```

`tests/run_tests.py` fails if fewer than the historical floor of tests actually
*execute*, so mass-skipping is treated as a failure rather than a pass.

## 3. Style and conventions

- Python 3.11, 4-space indentation, `snake_case`, module-level docstrings.
- Keep the 12-column article schema (`article_id … url`) unchanged; downstream
  tests and dashboards depend on it.
- New analysis code goes in `src/`, with a matching `tests/test_*.py`.
- Do not commit secrets. Copy `.env.example` to `.env` for local overrides;
  `.env` is git-ignored.
- Respect `.gitattributes` (LF for text, CRLF for `*.bat`, binary for media).

## 4. Data policy

Committed: raw CSVs, the Word report, the PowerPoint deck and the offline HTML
dashboards (so a fresh clone is inspectable).

Not committed: Parquet lakehouses, `data/streaming_parquet/`, MongoDB offline
collections and simulated HDFS block files — all regenerable via `run_all.py`.

## 5. Commit messages

Use a short imperative subject, then a body explaining *why*:

```
fix(ci): rebuild Parquet lakehouse when summary exists but lakehouse is absent

The summary JSON is committed but the lakehouse is git-ignored, so a fresh
checkout skipped the PySpark stage and the data tests failed.
```

## 6. Before opening a pull request

1. `python -m compileall -q src tests run_all.py`
2. `python tests/run_tests.py` — all tests executed, 0 failures
3. Regenerate any artifact whose source you changed
   (`python src/visualize_analytics.py`, `python src/generate_presentation_pptx.py`,
   `python src/export_report_docx.py`)
4. Update `PROJECT_REPORT.md`, `README.md` and `presentation/slide_deck_outline.md`
   together so reported numbers stay consistent.
