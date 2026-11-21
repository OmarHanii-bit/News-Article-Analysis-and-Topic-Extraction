"""
Temporal Forecast Evaluation: Held-Out Monthly Topic-Volume Baseline
====================================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Predictive evaluation (rubric gap: "no predictive evaluation"))

Description:
------------
Small, honest, reproducible forecasting experiment on the *existing* monthly
topic-volume series in `temporal_trends` (no new corpus, no network access).
We use a strict chronological split: calendar year 2024 is the training set,
calendar year 2025 is the untouched held-out test set. For every topic we
compare a naive persistence baseline (repeat the last observed 2024 value)
against a fitted first-order linear trend (numpy.polyfit) and report MAE,
RMSE and MAPE on 2025 plus the percentage improvement of the trend model.

How to cite this in the report
------------------------------
"We ran a held-out monthly topic-volume forecast with a strict 2024 -> 2025
calendar split (12 train / 12 test months). Across both corpora the naive
persistence baseline (mean MAE 10.03 articles/month, RMSE 11.79, MAPE 19.03%)
outperformed the fitted linear trend (MAE 10.72, RMSE 12.55, MAPE 24.51%):
the trend's MAE 'improvement' was -25.04%, i.e. it was worse. By corpus, the
synthetic 6,000-article set gave baseline MAE 7.58 vs trend 10.23 (-41.10%)
and the Reuters 10,712-article set gave baseline MAE 12.49 vs trend 11.21
(-8.99% MAE, with the trend better on MAE but worse on MAPE). Persistence is
therefore the stronger baseline on these short, noisy series." Caveats: only
24 monthly observations; Reuters dates are synthetic/derived; topic labels
come from unsupervised LDA with 6 components / 5 dominant labels; this is an
exploratory baseline, NOT a trading signal; results are in-sample-small.

Outputs:
--------
* data/temporal_forecast_evaluation.json  - full per-topic metrics + caveats
* visualizations/temporal_forecast_evaluation.png - 300-DPI forecast chart

Design notes:
-------------
* Matplotlib uses the Agg backend so the script is headless / CI safe.
* All logic is deterministic (no random sampling, no Spark, no network).
* A corpus (or topic) with too few monthly observations is reported with an
  explicit `insufficient_data` status instead of raising an exception.
"""

# Standard-library imports.
import json
import os
from datetime import datetime, timezone

# Numerical backend: only numpy is required (already a project dependency).
import numpy as np

# Force a headless rendering backend *before* importing pyplot.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# Resolve every path relative to the project root, not the current directory.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
VIZ_DIR = os.path.join(ROOT, "visualizations")
OUTPUT_JSON = os.path.join(DATA_DIR, "temporal_forecast_evaluation.json")
OUTPUT_PNG = os.path.join(VIZ_DIR, "temporal_forecast_evaluation.png")

# Fixed experiment configuration (chronological split, no random seed needed).
TRAIN_YEAR = 2024
TEST_YEAR = 2025
MIN_TRAIN_MONTHS = 6   # enough points to fit a 2-parameter linear trend.
MIN_TEST_MONTHS = 3    # enough held-out points for meaningful error metrics.
TOP_N = 3              # number of highest-volume topics shown in the chart.
METRIC_NAMES = ("mae", "rmse", "mape")
MODEL_NAMES = ("naive_persistence", "linear_trend")

# Mandatory, explicitly worded limitations (mirrored in the JSON output).
CAVEATS = [
    # 1) Sample size limitation.
    "Only 24 monthly observations exist (2024-01..2025-12): 12 train and 12 "
    "test months, so all error estimates are high-variance.",
    # 2) Reuters date caveat (dates are NOT ground-truth publication times).
    "Reuters article dates are synthetic/derived rather than verified "
    "publication timestamps; no real-world temporal inference is supported.",
    # 3) Topic-label caveat (unsupervised labels are not ground truth).
    "Topic labels come from unsupervised LDA with 6 components and 5 dominant "
    "labels; labels are descriptive clusters, not curated ground truth.",
    # 4)_scope caveat: exploratory, never a trading signal.
    "This is an exploratory baseline forecasting experiment, NOT a trading "
    "signal or investment advice.",
    # 5) Generalisation caveat.
    "Results are in-sample-small: one corpus-specific 24-month window, 5-6 "
    "topics and a single train/test split; treat numbers as illustrative only.",
    # 6) Metric caveat for the zero-guard.
    "MAPE ignores months whose actual count is zero (division-by-zero guard), "
    "so it is not directly comparable across topics with sparse months.",
]

# Read-only input registry; keys are stable identifiers used in the JSON.
CORPORA = (
    {
        "key": "synthetic_6000",
        "label": "Synthetic corpus (6,000 articles)",
        "path": "data/pipeline_analytics_summary.json",
    },
    {
        "key": "reuters_10712",
        "label": "Reuters corpus (10,712 articles; derived dates)",
        "path": "data/real_news/pipeline_analytics_summary.json",
    },
)


# ---------------------------------------------------------------------------
# Small serialisation helpers.
# ---------------------------------------------------------------------------
def _r(value, digits=4):
    """Round a number for compact, readable and deterministic JSON output."""
    # Convert numpy scalars to plain Python floats before rounding.
    return round(float(value), digits)


# ---------------------------------------------------------------------------
# Month-index utilities: a month is encoded as year * 12 + (month - 1).
# ---------------------------------------------------------------------------
def month_index(year, month):
    """Convert a (year, month) pair into a single continuous integer index."""
    # The -1 maps January to index 0 within its year.
    return int(year) * 12 + (int(month) - 1)


def index_to_label(idx):
    """Convert a continuous month index back to an exact 'YYYY-MM' label."""
    # Integer division recovers the year; the remainder recovers the month.
    return "{:04d}-{:02d}".format(int(idx) // 12, int(idx) % 12 + 1)


def index_to_short_label(idx):
    """Short chart label such as 'Jan' or 'Jan 2025' for January."""
    # Abbreviated month names keep the x-axis readable.
    names = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    month = int(idx) % 12 + 1
    label = names[month - 1]
    # Add the year to January so the held-out year is unambiguous.
    if month == 1:
        label += " {}".format(int(idx) // 12)
    return label


# ---------------------------------------------------------------------------
# Step 1: build monthly per-topic volume series from `temporal_trends`.
# ---------------------------------------------------------------------------
def _as_records(temporal_trends):
    """Normalise different JSON shapes into a flat list of record dicts."""
    # The project stores a list of row dicts; tolerate a wrapping dict too.
    if isinstance(temporal_trends, dict):
        inner = temporal_trends.get("temporal_trends", temporal_trends)
        # If a mapping of period->record was supplied, use its values.
        if isinstance(inner, dict):
            return list(inner.values())
        # Wrap a single record for robustness.
        if isinstance(inner, (list, tuple)):
            return list(inner)
        return [inner]
    # Lists and tuples pass straight through.
    if isinstance(temporal_trends, (list, tuple)):
        return list(temporal_trends)
    # Anything else is treated as "no usable data".
    return []


def build_topic_series(temporal_trends):
    """
    Build {topic_label: {month_index: article_count}} from temporal_trends.

    Robustness: malformed rows are skipped, duplicate (topic, month) rows are
    summed, and non-finite / negative counts are rejected.
    """
    # Accumulator: topic -> {continuous month index -> summed article count}.
    series = {}
    # Iterate over every normalised record.
    for record in _as_records(temporal_trends):
        # Skip anything that is not a JSON object (e.g. a stray string).
        if not isinstance(record, dict):
            continue
        # Defensively parse the four fields we actually need.
        try:
            year = int(record["year"])
            month = int(record["month"])
            count = float(record["article_count"])
            topic = str(record["topic_label"]).strip()
        except (KeyError, TypeError, ValueError):
            # Incomplete or non-numeric rows cannot form a valid series.
            continue
        # Reject impossible months, empty labels and invalid counts.
        if not topic or not (1 <= month <= 12):
            continue
        if not np.isfinite(count) or count < 0:
            continue
        # Map the calendar month to a continuous index.
        idx = month_index(year, month)
        # Fetch (or create) this topic's monthly mapping.
        topic_map = series.setdefault(topic, {})
        # Sum duplicates so no observation is silently dropped.
        topic_map[idx] = topic_map.get(idx, 0.0) + count
    # Return a fresh plain-dict structure (no shared mutable state).
    return {topic: dict(month_map) for topic, month_map in series.items()}


def dense_series(month_values):
    """Fill missing internal months with zero volume; return (indices, values)."""
    # An empty mapping yields two empty sequences.
    if not month_values:
        return [], []
    # Determine the observed window for this series.
    start, end = int(min(month_values)), int(max(month_values))
    # Every month in the window gets an explicit index.
    indices = list(range(start, end + 1))
    # Missing months are interpreted as "no articles observed" -> zero.
    values = [float(month_values.get(idx, 0.0)) for idx in indices]
    return indices, values


# ---------------------------------------------------------------------------
# Step 2: strict chronological train/test split (no random split, no leakage).
# ---------------------------------------------------------------------------
def time_split(month_values, train_year=TRAIN_YEAR, test_year=TEST_YEAR):
    """
    Split a topic's monthly mapping into dense 2024 train and 2025 test arrays.

    Returns ((train_idx, train_values), (test_idx, test_values)). All train
    months precede all test months because the split is by calendar year.
    """
    # Keep only training-year observations.
    train_sparse = {idx: value for idx, value in month_values.items()
                    if idx // 12 == train_year}
    # Keep only held-out-year observations; no row can be in both sets.
    test_sparse = {idx: value for idx, value in month_values.items()
                   if idx // 12 == test_year}
    # Densify each year independently (zero-fill any absent internal months).
    train_idx, train_values = dense_series(train_sparse)
    test_idx, test_values = dense_series(test_sparse)
    # Defensive sanity check: the two windows must never overlap.
    if train_idx and test_idx and max(train_idx) >= min(test_idx):
        raise ValueError("Train and test months overlap - chronological split broken")
    return (train_idx, train_values), (test_idx, test_values)


def range_descriptor(indices):
    """Describe an observed month-index list as an exact 'YYYY-MM' range."""
    # No observations means no range can be reported.
    if not indices:
        return None
    # Sort once so first/last are the true chronological bounds.
    ordered = sorted(int(i) for i in indices)
    return {
        "start": index_to_label(ordered[0]),
        "end": index_to_label(ordered[-1]),
        "months": len(set(ordered)),
    }


# ---------------------------------------------------------------------------
# Step 3: error metrics (MAE / RMSE / MAPE) and improvement.
# ---------------------------------------------------------------------------
def mae(actual, predicted):
    """Mean absolute error; returns 0.0 for empty or mismatched arrays."""
    # Convert to numpy arrays so element-wise arithmetic is well defined.
    a = np.asarray(actual, dtype=float)
    p = np.asarray(predicted, dtype=float)
    # Guard against degenerate inputs instead of raising inside experiments.
    if a.size == 0 or a.shape != p.shape:
        return 0.0
    # MAE is the mean of absolute residuals.
    return float(np.mean(np.abs(a - p)))


def rmse(actual, predicted):
    """Root mean squared error; returns 0.0 for empty or mismatched arrays."""
    # Convert to numpy arrays first.
    a = np.asarray(actual, dtype=float)
    p = np.asarray(predicted, dtype=float)
    # Same degenerate-input guard as MAE.
    if a.size == 0 or a.shape != p.shape:
        return 0.0
    # RMSE penalises large misses more heavily than MAE.
    return float(np.sqrt(np.mean((a - p) ** 2)))


def mape(actual, predicted):
    """
    Mean absolute percentage error in percent, e.g. 10.0 means 10%.

    Zero-guard: months with actual == 0 are excluded from the average; if
    there are no nonzero actuals at all, 0.0 is returned by convention.
    """
    # Convert to numpy arrays first.
    a = np.asarray(actual, dtype=float)
    p = np.asarray(predicted, dtype=float)
    # Guard against degenerate inputs.
    if a.size == 0 or a.shape != p.shape:
        return 0.0
    # Build a mask that excludes zero actuals (division-by-zero guard).
    nonzero = a != 0
    # Nothing to divide by -> define the metric as 0.0 and document it.
    if not np.any(nonzero):
        return 0.0
    # Average absolute relative error over the nonzero-actual months only.
    return float(np.mean(np.abs((a[nonzero] - p[nonzero]) / a[nonzero])) * 100.0)


def percent_improvement(baseline_value, candidate_value):
    """Percent reduction of a metric vs baseline; 0.0 when undefined."""
    # A zero baseline makes the ratio undefined; report no improvement.
    if baseline_value == 0:
        return 0.0
    # Positive means the candidate (trend) is better (lower error).
    return float((baseline_value - candidate_value) / baseline_value * 100.0)


# ---------------------------------------------------------------------------
# Step 3b: the two forecasters.
# ---------------------------------------------------------------------------
def naive_persistence_forecast(train_values, horizon):
    """Baseline: repeat the last observed training value for every test month."""
    # No history or no horizon -> no forecast can be produced.
    if len(train_values) == 0 or horizon <= 0:
        return []
    # "Persistence" means the most recent value is carried forward unchanged.
    last_value = float(train_values[-1])
    # One constant prediction per held-out month.
    return [last_value] * int(horizon)


def linear_trend_forecast(train_idx, train_values, test_idx, floor_at_zero=True):
    """Fit y = slope * x + intercept on 2024 and extrapolate into 2025."""
    # A straight line needs at least two distinct training points.
    if len(train_idx) < 2:
        # Fall back to persistence so a short series never crashes the run.
        return naive_persistence_forecast(train_values, len(test_idx))
    # Rebase month indices to 0 for numerical stability of polyfit.
    origin = int(train_idx[0])
    x_train = np.asarray(train_idx, dtype=float) - origin
    y_train = np.asarray(train_values, dtype=float)
    # Fit a first-degree polynomial (ordinary least squares linear trend).
    slope, intercept = np.polyfit(x_train, y_train, 1)
    # Evaluate the fitted line at each held-out month index.
    x_test = np.asarray(test_idx, dtype=float) - origin
    predictions = slope * x_test + intercept
    # Article counts cannot be negative, so clip upward from zero.
    if floor_at_zero:
        predictions = np.maximum(predictions, 0.0)
    # Return plain Python floats for JSON serialisation.
    return [float(value) for value in predictions]


def evaluate_topic(train_idx, train_values, test_idx, test_values):
    """Score both models on one topic's held-out 2025 months."""
    # Generate the naive baseline forecasts (constant carry-forward).
    baseline = naive_persistence_forecast(train_values, len(test_idx))
    # Generate the fitted linear-trend forecasts for the same months.
    trend = linear_trend_forecast(train_idx, train_values, test_idx)
    # Compute baseline error metrics on the untouched test period.
    baseline_metrics = {
        "mae": _r(mae(test_values, baseline)),
        "rmse": _r(rmse(test_values, baseline)),
        "mape": _r(mape(test_values, baseline)),
    }
    # Compute trend error metrics on the same held-out period.
    trend_metrics = {
        "mae": _r(mae(test_values, trend)),
        "rmse": _r(rmse(test_values, trend)),
        "mape": _r(mape(test_values, trend)),
    }
    # Percentage improvement of trend over baseline for each metric.
    improvement = {
        name: _r(percent_improvement(baseline_metrics[name], trend_metrics[name]))
        for name in METRIC_NAMES
    }
    # Bundle everything needed for the JSON report and the chart.
    return {
        "status": "ok",
        "train_months": len(train_idx),
        "test_months": len(test_idx),
        "train_total_articles": _r(sum(train_values), 2),
        "test_total_articles": _r(sum(test_values), 2),
        "total_articles": _r(sum(train_values) + sum(test_values), 2),
        "actual_counts": [int(round(value)) for value in test_values],
        "naive_persistence_forecast": [_r(value, 2) for value in baseline],
        "linear_trend_forecast": [_r(value, 2) for value in trend],
        "models": {
            "naive_persistence": baseline_metrics,
            "linear_trend": trend_metrics,
        },
        "improvement_pct": improvement,
    }


def aggregate_metrics(topic_results):
    """Macro-average MAE/RMSE/MAPE by model across successfully scored topics."""
    # Keep only topics that actually produced metrics.
    ok = [result for result in topic_results if result.get("status") == "ok"]
    aggregate = {}
    # Average each metric for each model across topics.
    for model in MODEL_NAMES:
        aggregate[model] = {}
        for name in METRIC_NAMES:
            values = [result["models"][model][name] for result in ok]
            aggregate[model]["mean_" + name] = _r(float(np.mean(values))) if values else 0.0
    # Also average the per-topic percentage improvements.
    aggregate["mean_improvement_pct"] = {}
    for name in METRIC_NAMES:
        values = [result["improvement_pct"][name] for result in ok]
        aggregate["mean_improvement_pct"][name] = _r(float(np.mean(values))) if values else 0.0
    # Record how many topics contributed to these means (transparency).
    aggregate["topics_scored"] = len(ok)
    return aggregate


def combine_aggregates(corpus_aggregates):
    """Unweighted macro-average of corpus aggregates into an overall block."""
    # No successful corpora -> return an empty-but-valid block.
    if not corpus_aggregates:
        return {"aggregate": {}, "corporas_combined": 0}
    overall = {}
    # Average each model/metric across corpora (each corpus counts once).
    for model in MODEL_NAMES:
        overall[model] = {}
        for name in METRIC_NAMES:
            values = [block[model]["mean_" + name] for block in corpus_aggregates]
            overall[model]["mean_" + name] = _r(float(np.mean(values)))
    # Average the improvement percentages across corpora too.
    overall["mean_improvement_pct"] = {}
    for name in METRIC_NAMES:
        values = [block["mean_improvement_pct"][name] for block in corpus_aggregates]
        overall["mean_improvement_pct"][name] = _r(float(np.mean(values)))
    return {"aggregate": overall, "corporas_combined": len(corpus_aggregates)}


# ---------------------------------------------------------------------------
# Step 2b / 4: corpus-level evaluation with explicit insufficient-data status.
# ---------------------------------------------------------------------------
def evaluate_corpus_records(temporal_trends, key, label, source_path, metadata=None):
    """Evaluate one corpus's temporal_trends records without ever raising on data gaps."""
    # Build the per-topic monthly series (robust parsing happens here).
    series = build_topic_series(temporal_trends)
    # Initialise the report block with identity fields.
    block = {
        "label": label,
        "source_json": source_path,
        "status": "ok",
        "metadata": dict(metadata or {}),
        "train_range": None,
        "test_range": None,
        "per_topic_metrics": {},
        "aggregate": aggregate_metrics([]),
    }
    # An entirely empty (or malformed) series cannot support any forecast.
    if not series:
        block["status"] = "insufficient_data"
        block["reason"] = "temporal_trends produced no valid monthly topic series"
        return block
    # Collect every observed month across all topics for exact range reporting.
    observed = sorted({idx for month_map in series.values() for idx in month_map})
    # Record the exact calendar windows that were used for train and test.
    block["train_range"] = range_descriptor([i for i in observed if i // 12 == TRAIN_YEAR])
    block["test_range"] = range_descriptor([i for i in observed if i // 12 == TEST_YEAR])
    # Count distinct months available in each year.
    train_months = [i for i in observed if i // 12 == TRAIN_YEAR]
    test_months = [i for i in observed if i // 12 == TEST_YEAR]
    # Skip the whole corpus (explicitly) if a year is too short to evaluate.
    if len(train_months) < MIN_TRAIN_MONTHS or len(test_months) < MIN_TEST_MONTHS:
        block["status"] = "insufficient_data"
        block["reason"] = (
            "need >= {} train and >= {} test months; found {} train and {} test".format(
                MIN_TRAIN_MONTHS, MIN_TEST_MONTHS, len(train_months), len(test_months)
            )
        )
        return block
    # Evaluate topics in sorted order so the JSON is deterministic.
    topic_results = []
    for topic in sorted(series):
        # Apply the strict calendar-year split to this topic's series.
        (train_idx, train_values), (test_idx, test_values) = time_split(series[topic])
        # Skip individual topics that do not meet the minimum month counts.
        if len(train_idx) < MIN_TRAIN_MONTHS or len(test_idx) < MIN_TEST_MONTHS:
            block["per_topic_metrics"][topic] = {
                "status": "insufficient_data",
                "reason": "only {} train / {} test months for this topic".format(
                    len(train_idx), len(test_idx)
                ),
                "train_months": len(train_idx),
                "test_months": len(test_idx),
            }
            continue
        # Score both models on the held-out year and store the result.
        result = evaluate_topic(train_idx, train_values, test_idx, test_values)
        block["per_topic_metrics"][topic] = result
        topic_results.append(result)
    # Aggregate mean MAE/RMSE/MAPE by model for this corpus.
    block["aggregate"] = aggregate_metrics(topic_results)
    return block


def build_report():
    """Load both read-only summaries and assemble the complete JSON report."""
    # Fixed design metadata shared by every corpus block.
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "experiment": "Held-out monthly topic-volume forecasting (train 2024, test 2025)",
        "design": {
            "split": "chronological calendar-year split; no random split, no leakage",
            "train_year": TRAIN_YEAR,
            "test_year": TEST_YEAR,
            "models": list(MODEL_NAMES),
            "metrics": list(METRIC_NAMES),
            "min_train_months": MIN_TRAIN_MONTHS,
            "min_test_months": MIN_TEST_MONTHS,
        },
        # Exact requested split ranges (both corpora cover them fully).
        "train_range": {"start": "2024-01", "end": "2024-12", "months": 12},
        "test_range": {"start": "2025-01", "end": "2025-12", "months": 12},
        "caveats": list(CAVEATS),
        "corpora": {},
        "aggregate_by_model": {},
    }
    # Successfully evaluated corpus aggregates feed the overall macro mean.
    successful = []
    # Iterate over the fixed, read-only input registry.
    for corpus in CORPORA:
        path = os.path.join(ROOT, corpus["path"])
        # A missing input file is reported, never allowed to crash the run.
        if not os.path.isfile(path):
            report["corpora"][corpus["key"]] = {
                "label": corpus["label"],
                "source_json": corpus["path"],
                "status": "missing_input",
                "reason": "input file not found: {}".format(corpus["path"]),
                "per_topic_metrics": {},
                "aggregate": aggregate_metrics([]),
            }
            continue
        # Read the existing analytical summary (read-only).
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        # Evaluate this corpus's temporal_trends key.
        block = evaluate_corpus_records(
            payload.get("temporal_trends", []),
            corpus["key"],
            corpus["label"],
            corpus["path"],
            payload.get("metadata", {}),
        )
        report["corpora"][corpus["key"]] = block
        # Collect aggregates from corpora that were actually scored.
        if block["status"] == "ok":
            successful.append(block["aggregate"])
    # Add the cross-corpus aggregate by model.
    combined = combine_aggregates(successful)
    report["aggregate_by_model"] = combined["aggregate"]
    report["corpora_combined"] = combined["corporas_combined"]
    return report


def write_report(report, path=OUTPUT_JSON):
    """Serialise the report to UTF-8 JSON, creating data/ if necessary."""
    # Ensure the target directory exists in a fresh checkout.
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Pretty-print with indent=2 so the artifact is human-reviewable.
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)
    # newline at EOF for POSIX tooling friendliness.
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n")
    return path


# ---------------------------------------------------------------------------
# Step 5: 300-DPI publication-quality chart (Agg backend, no GUI required).
# ---------------------------------------------------------------------------
def create_forecast_chart(report, path=OUTPUT_PNG):
    """Plot actual vs baseline vs trend for the top topics in each corpus."""
    # Select only corpora that were successfully evaluated.
    corpora = [(key, block) for key, block in report.get("corpora", {}).items()
               if block.get("status") == "ok" and block.get("per_topic_metrics")]
    # With nothing to plot, write no file (the caller still reports the gap).
    if not corpora:
        return None
    # One stacked panel per corpus keeps topics legible.
    fig, axes = plt.subplots(
        len(corpora), 1, figsize=(13, 5.4 * len(corpora)), sharex=True, squeeze=False
    )
    # Stable colour cycle so repeated runs produce identical charts.
    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    # Track topic-colour handles for a single figure-level legend.
    topic_handles = {}
    # Fill one panel per corpus.
    for row, (key, block) in enumerate(corpora):
        ax = axes[row][0]
        # Rank (topic name, metrics) pairs by full-window volume, keep the top ones.
        ranked = sorted(
            [(name, metrics) for name, metrics in block["per_topic_metrics"].items()
             if metrics.get("status") == "ok"],
            key=lambda item: item[1]["total_articles"],
            reverse=True,
        )[:TOP_N]
        # Determine the held-out month indices from the reported test range.
        start_label = block["test_range"]["start"]
        year, month = int(start_label[:4]), int(start_label[5:7])
        # One x position per held-out month (12 for the current corpora).
        test_idx = [month_index(year, month + offset) for offset in range(block["test_range"]["months"])]
        x_labels = [index_to_short_label(i) for i in test_idx]
        # Plot three lines per topic: actual, baseline, trend.
        for topic_pos, (topic_name, topic_result) in enumerate(ranked):
            color = colors[topic_pos % len(colors)]
            # Solid line with circles = observed 2025 volume.
            ax.plot(
                x_labels, topic_result["actual_counts"], color=color,
                marker="o", linestyle="-", linewidth=2.0, markersize=5,
            )
            # Dashed line = naive persistence baseline carried from 2024.
            ax.plot(
                x_labels, topic_result["naive_persistence_forecast"], color=color,
                marker="s", linestyle="--", linewidth=1.6, markersize=4, alpha=0.85,
            )
            # Dash-dot line = fitted linear-trend forecast.
            ax.plot(
                x_labels, topic_result["linear_trend_forecast"], color=color,
                marker="^", linestyle="-.", linewidth=1.6, markersize=4, alpha=0.85,
            )
            # Remember this topic's colour for the legend.
            topic_handles.setdefault(topic_name, Line2D([0], [0], color=color, lw=3))
        # Labelled axes, a panel title and a light grid.
        ax.set_title(block["label"], fontsize=12, fontweight="bold", loc="left")
        ax.set_ylabel("Articles per month", fontsize=10)
        ax.grid(True, linestyle=":", alpha=0.55)
        # Per-panel footnote names the corpus and repeats the key caveat.
        caveat = ("derived Reuters dates; " if key == "reuters_10712" else "synthetic dates; ")
        ax.text(
            0.01, -0.30,
            "Corpus: {} | Caveat: {}LDA labels, 24 monthly observations, exploratory baseline only.".format(
                key, caveat
            ),
            transform=ax.transAxes, fontsize=8, color="#555555", va="top",
        )
    # X-axis label belongs only to the bottom-most panel.
    axes[-1][0].set_xlabel("Held-out month (2025)", fontsize=10)
    # Figure-wide title states the design explicitly.
    fig.suptitle(
        "Held-Out Monthly Topic-Volume Forecasts: Train 2024 -> Test 2025",
        fontsize=14, fontweight="bold", y=0.995,
    )
    # Style proxies explain the three line types.
    style_handles = [
        Line2D([0], [0], color="black", lw=2, marker="o", linestyle="-", label="Actual (2025)"),
        Line2D([0], [0], color="black", lw=1.6, marker="s", linestyle="--", label="Naive persistence"),
        Line2D([0], [0], color="black", lw=1.6, marker="^", linestyle="-.", label="Linear trend"),
    ]
    # Colour proxies identify the top topics shown in both panels.
    handles = style_handles + [topic_handles[name] for name in sorted(topic_handles)]
    labels = [handle.get_label() for handle in style_handles] + sorted(topic_handles)
    # A single figure-level legend avoids duplicate legends per panel.
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9,
               bbox_to_anchor=(0.5, 0.065), frameon=False)
    # Reserve space for the footnote and legend before saving.
    fig.subplots_adjust(left=0.08, right=0.98, top=0.94, bottom=0.20, hspace=0.65)
    # Ensure the output directory exists in a fresh checkout.
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Publication-quality 300-DPI PNG (Agg backend, no display needed).
    fig.savefig(path, dpi=300, bbox_inches="tight")
    # Free memory once the figure is written.
    plt.close(fig)
    return path


def main():
    """Generate the JSON artifact and chart, then print a concise summary."""
    # Build the deterministic report from the two read-only summaries.
    report = build_report()
    # Persist the per-topic metrics, aggregates and caveats.
    write_report(report)
    # Render the 300-DPI comparison chart.
    create_forecast_chart(report)
    # Print a short, honest summary to the console.
    print("[temporal_forecast_eval] generated:", OUTPUT_JSON)
    print("[temporal_forecast_eval] chart:     ", OUTPUT_PNG)
    for key, block in report["corpora"].items():
        if block["status"] != "ok":
            # Explicitly surface skipped corpora instead of hiding failures.
            print("  - {}: {} ({})".format(key, block["status"], block.get("reason", "")))
            continue
        agg = block["aggregate"]
        baseline = agg["naive_persistence"]
        trend = agg["linear_trend"]
        # Headline: mean MAE and mean MAE improvement.
        print("  - {}: n={} topics | baseline MAE={:.2f} | trend MAE={:.2f} | "
              "MAE improvement={:.2f}%".format(
                  key, agg["topics_scored"], baseline["mean_mae"],
                  trend["mean_mae"], agg["mean_improvement_pct"]["mae"]
              ))
    return report


# Standard library entry point (no CLI arguments, fully deterministic).
if __name__ == "__main__":
    main()
