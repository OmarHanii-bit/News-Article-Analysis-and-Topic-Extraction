"""
Unit Tests: Temporal Forecast Evaluation (held-out monthly topic volumes)
=========================================================================
Component: Predictive evaluation QA
Scope: fast, offline, deterministic helpers only -- no Spark, no network.

These tests exercise the helper functions of `src/temporal_forecast_eval.py`
with small synthetic dictionaries whose expected values are known exactly.
"""

# Standard-library imports.
import json
import math
import os
import sys
import unittest

# Make the project's src/ importable without executing any pipeline code.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import temporal_forecast_eval as tfe  # noqa: E402  (path setup must come first)


class TestTemporalForecastEval(unittest.TestCase):
    """Verify series construction, metrics, time splitting and JSON schema."""

    def test_build_topic_series_from_synthetic_dict(self):
        """Series are built per topic, duplicates are summed, bad rows skipped."""
        # Arrange: a small list of record dicts mimicking `temporal_trends`.
        records = [
            {"year": 2024, "month": 1, "topic_label": "A", "article_count": 10},
            {"year": 2024, "month": 1, "topic_label": "A", "article_count": 5},
            {"year": 2024, "month": 2, "topic_label": "A", "article_count": 7},
            {"year": 2025, "month": 1, "topic_label": "B", "article_count": 3},
            {"year": 2024, "month": "bad", "topic_label": "C", "article_count": 1},
            {"year": 2024, "month": 13, "topic_label": "D", "article_count": 1},
            {"year": 2024, "month": 3, "topic_label": "", "article_count": 1},
        ]
        # Act: build the per-topic monthly mapping.
        series = tfe.build_topic_series(records)
        # Assert: only valid topics survive.
        self.assertEqual(set(series), {"A", "B"})
        # Duplicate January rows for topic A are summed (10 + 5).
        self.assertEqual(series["A"][tfe.month_index(2024, 1)], 15.0)
        self.assertEqual(series["A"][tfe.month_index(2024, 2)], 7.0)
        self.assertEqual(series["B"][tfe.month_index(2025, 1)], 3.0)
        # A dict wrapper around the list is accepted as well.
        wrapped = tfe.build_topic_series({"temporal_trends": records})
        self.assertEqual(wrapped, series)

    def test_metrics_with_known_values(self):
        """MAE, RMSE and MAPE match hand-computed values."""
        # actual = [100, 200], predicted = [110, 180]
        # absolute errors = [10, 20] -> MAE = 15
        # squared errors = [100, 400] -> RMSE = sqrt(250)
        # relative errors = [10%, 10%] -> MAPE = 10.0 (percent)
        actual = [100.0, 200.0]
        predicted = [110.0, 180.0]
        self.assertAlmostEqual(tfe.mae(actual, predicted), 15.0, places=6)
        self.assertAlmostEqual(tfe.rmse(actual, predicted), math.sqrt(250.0), places=6)
        self.assertAlmostEqual(tfe.mape(actual, predicted), 10.0, places=6)

    def test_mape_zero_guard(self):
        """Zero actuals are excluded; all-zero actuals return 0.0 by convention."""
        # The zero-actual month is ignored even though its prediction is huge.
        self.assertAlmostEqual(tfe.mape([0.0, 100.0], [999.0, 110.0]), 10.0, places=6)
        # With no nonzero actuals there is nothing to divide by.
        self.assertEqual(tfe.mape([0.0, 0.0], [1.0, 2.0]), 0.0)
        # Empty or mismatched inputs are also safe.
        self.assertEqual(tfe.mape([], []), 0.0)
        self.assertEqual(tfe.mape([1.0], [1.0, 2.0]), 0.0)

    def test_time_split_correctness(self):
        """Train 2024 and test 2025 months are dense, disjoint and ordered."""
        # Sparse inputs with internal gaps in both calendar years.
        month_values = {
            tfe.month_index(2024, 1): 5.0,
            tfe.month_index(2024, 3): 7.0,
            tfe.month_index(2025, 1): 9.0,
            tfe.month_index(2025, 12): 11.0,
        }
        # Act: apply the strict calendar-year split.
        (train_idx, train_values), (test_idx, test_values) = tfe.time_split(month_values)
        # Each year is densified across its *observed* window (Jan-Mar / Jan-Dec).
        self.assertEqual(train_idx, [tfe.month_index(2024, m) for m in range(1, 4)])
        self.assertEqual(test_idx, [tfe.month_index(2025, m) for m in range(1, 13)])
        # Internal gaps are zero-filled, observed values are preserved.
        self.assertEqual(train_values, [5.0, 0.0, 7.0])
        self.assertEqual(test_values[0], 9.0)
        self.assertEqual(test_values[10], 0.0)
        self.assertEqual(test_values[11], 11.0)
        # No leakage: every training month strictly precedes every test month.
        self.assertLess(max(train_idx), min(test_idx))

    def test_forecast_models_are_deterministic(self):
        """Persistence repeats the last value; trend extrapolates a fitted line."""
        # Naive baseline: carry forward December's value for 3 months.
        self.assertEqual(tfe.naive_persistence_forecast([4.0, 8.0, 12.0], 3), [12.0] * 3)
        self.assertEqual(tfe.naive_persistence_forecast([], 3), [])
        # A perfectly linear training series must extrapolate exactly.
        trend = tfe.linear_trend_forecast([0, 1, 2], [10.0, 20.0, 30.0], [3, 4])
        self.assertAlmostEqual(trend[0], 40.0, places=6)
        self.assertAlmostEqual(trend[1], 50.0, places=6)
        # Declining series would go negative: counts are clipped at zero.
        clipped = tfe.linear_trend_forecast([0, 1, 2], [10.0, 8.0, 6.0], [10, 12])
        self.assertTrue(all(value >= 0.0 for value in clipped))
        # Repeated calls with identical inputs are byte-for-byte identical.
        self.assertEqual(
            tfe.linear_trend_forecast([0, 1, 2], [10.0, 20.0, 30.0], [3, 4]), trend
        )

    def test_percent_improvement_and_aggregate(self):
        """Improvement signs and macro-averaged model metrics are correct."""
        # Lower error is better: 20 -> 15 is a 25% improvement.
        self.assertAlmostEqual(tfe.percent_improvement(20.0, 15.0), 25.0, places=6)
        # 20 -> 25 is a 25% regression (negative improvement).
        self.assertAlmostEqual(tfe.percent_improvement(20.0, 25.0), -25.0, places=6)
        # A zero baseline makes the ratio undefined -> 0.0 by convention.
        self.assertEqual(tfe.percent_improvement(0.0, 10.0), 0.0)
        # Aggregate two fake topics with known per-model metrics.
        topics = [
            {"status": "ok", "models": {"naive_persistence": {"mae": 4.0, "rmse": 5.0, "mape": 10.0},
                                        "linear_trend": {"mae": 2.0, "rmse": 3.0, "mape": 6.0}},
             "improvement_pct": {"mae": 50.0, "rmse": 40.0, "mape": 40.0}},
            {"status": "ok", "models": {"naive_persistence": {"mae": 6.0, "rmse": 7.0, "mape": 20.0},
                                        "linear_trend": {"mae": 4.0, "rmse": 5.0, "mape": 14.0}},
             "improvement_pct": {"mae": 33.3333, "rmse": 28.5714, "mape": 30.0}},
        ]
        agg = tfe.aggregate_metrics(topics)
        self.assertEqual(agg["topics_scored"], 2)
        self.assertAlmostEqual(agg["naive_persistence"]["mean_mae"], 5.0, places=4)
        self.assertAlmostEqual(agg["linear_trend"]["mean_rmse"], 4.0, places=4)
        self.assertAlmostEqual(agg["mean_improvement_pct"]["mae"], 41.6667, places=3)

    def test_insufficient_data_status_never_crashes(self):
        """Short corpora are skipped with an explicit insufficient_data status."""
        # Only 2 train months and 1 test month -> below configured minimums.
        short_records = [
            {"year": 2024, "month": 1, "topic_label": "A", "article_count": 10},
            {"year": 2024, "month": 2, "topic_label": "A", "article_count": 12},
            {"year": 2025, "month": 1, "topic_label": "A", "article_count": 11},
        ]
        block = tfe.evaluate_corpus_records(short_records, "short", "Short corpus", "none.json")
        self.assertEqual(block["status"], "insufficient_data")
        self.assertIn("need >=", block["reason"])
        self.assertEqual(block["per_topic_metrics"], {})
        # An entirely empty temporal_trends list is equally safe.
        empty = tfe.evaluate_corpus_records([], "empty", "Empty corpus", "none.json")
        self.assertEqual(empty["status"], "insufficient_data")
        self.assertIn("no valid monthly topic series", empty["reason"])

    def test_report_json_schema_keys(self):
        """Generated JSON exposes all required keys, ranges, metrics and caveats."""
        # Prefer the on-disk artifact; fall back to an in-memory build if absent.
        json_path = os.path.join(ROOT, "data", "temporal_forecast_evaluation.json")
        if os.path.isfile(json_path):
            with open(json_path, "r", encoding="utf-8") as handle:
                report = json.load(handle)
        else:
            report = tfe.build_report()
        # Top-level schema.
        for key in ("generated_at", "experiment", "design", "train_range", "test_range",
                    "caveats", "corpora", "aggregate_by_model"):
            self.assertIn(key, report)
        # Exact train/test date ranges.
        self.assertEqual(report["train_range"], {"start": "2024-01", "end": "2024-12", "months": 12})
        self.assertEqual(report["test_range"], {"start": "2025-01", "end": "2025-12", "months": 12})
        # Mandatory caveats are all present (checked by keyword).
        caveat_text = " ".join(report["caveats"]).lower()
        for keyword in ("24 monthly", "reuters", "synthetic/derived", "lda", "6 components",
                        "trading signal", "in-sample-small"):
            self.assertIn(keyword, caveat_text)
        # Both corpora and their metric blocks exist.
        self.assertIn("synthetic_6000", report["corpora"])
        self.assertIn("reuters_10712", report["corpora"])
        for block in report["corpora"].values():
            self.assertIn("status", block)
            self.assertIn("per_topic_metrics", block)
            self.assertIn("aggregate", block)
            if block["status"] != "ok":
                continue
            self.assertIn("train_range", block)
            self.assertIn("test_range", block)
            for metrics in block["per_topic_metrics"].values():
                if metrics.get("status") != "ok":
                    continue
                self.assertIn("models", metrics)
                self.assertIn("improvement_pct", metrics)
                for model in ("naive_persistence", "linear_trend"):
                    for metric in ("mae", "rmse", "mape"):
                        self.assertIn(metric, metrics["models"][model])
        # Aggregate mean MAE/RMSE/MAPE by model at overall and corpus level.
        for aggregate in [report["aggregate_by_model"]] + [c["aggregate"] for c in report["corpora"].values()]:
            for model in ("naive_persistence", "linear_trend"):
                for metric in ("mae", "rmse", "mape"):
                    self.assertIn("mean_" + metric, aggregate[model])


if __name__ == "__main__":
    unittest.main()
