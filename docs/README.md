# Documentation Index

Start with the top-level [`README.md`](../README.md) for setup and the
[`PROJECT_REPORT.md`](../PROJECT_REPORT.md) for the full academic write-up.
This folder holds focused technical notes that would otherwise bloat the report.

| Document | Purpose | Related report section |
|---|---|---|
| [`SCALABILITY_AND_KAGGLE.md`](SCALABILITY_AND_KAGGLE.md) | Measured 50k-article benchmark, projection method, and Kaggle ingestion commands | §5.5, §8 |
| [`../PROJECT_REPORT.md`](../PROJECT_REPORT.md) | Full capstone report (rubric alignment, architecture, evaluation, limitations, references) | — |
| [`../README.md`](../README.md) | Quick start, repository map, execution methods, evidence summary | §8 |
| [`../presentation/slide_deck_outline.md`](../presentation/slide_deck_outline.md) | 17-slide speaker notes, demo runbook and Q&A defense answers | §7 |
| [`../CONTRIBUTING.md`](../CONTRIBUTING.md) | Local setup, test protocol, data policy and PR checklist | §8 |
| [`../CHANGELOG.md`](../CHANGELOG.md) | Version history of features and fixes | — |

## Data and evidence artifacts

Machine-readable evidence lives next to the code so claims can be checked
without re-running the pipeline:

- `data/lda_k_sweep.json` and `data/lda_selection_note.md` — topic-count sweep.
- `data/temporal_forecast_evaluation.json` — held-out 2024→2025 forecast.
- `data/scale_benchmark.json` — throughput measurements and labelled projections.
- `data/supervised_classification_metrics.json` — Reuters ground-truth results.
- `data/topic_drift_analysis.json` — quarterly JSD/PSI monitoring.
- `data/real_news/live_rss_provenance.json` — live-feed provenance.
- `data/visualization_manifest.json` — inventory of the 14 static charts.

## Regenerating documentation

```bash
python src/generate_presentation_pptx.py   # rebuilds the 17-slide deck
python src/export_report_docx.py           # rebuilds PROJECT_REPORT.docx
python scripts/reexecute_notebook.py       # re-executes the notebook in place
```
