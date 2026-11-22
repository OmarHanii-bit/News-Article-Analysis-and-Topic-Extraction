"""
Unit & Regression Tests for MLOps Topic Drift Monitor
=====================================================
Author: Shehab Osama (NLP & Machine Learning Scientist)
Collaborator: Mazen Salem (QA, Benchmarking & Presentation Lead)
"""

import os
import json
import unittest


class TestTopicDriftMonitor(unittest.TestCase):
    """Validates topic drift evaluation metrics and visual artifacts."""

    def setUp(self):
        self.drift_json = "data/topic_drift_analysis.json"
        self.drift_png = "visualizations/topic_drift_monitoring.png"

    def test_drift_analysis_json_exists(self):
        """Drift analysis JSON must record quarterly trajectories and JSD metrics."""
        self.assertTrue(os.path.exists(self.drift_json), f"Missing {self.drift_json}")
        with open(self.drift_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertIn("monitoring_metric", data)
        self.assertIn("quarterly_drift_history", data)
        self.assertGreaterEqual(len(data["quarterly_drift_history"]), 4)

        # Check fields of each quarterly evaluation
        q1 = data["quarterly_drift_history"][0]
        self.assertIn("jsd_vs_baseline", q1)
        self.assertIn("psi_vs_baseline", q1)
        self.assertIn("drift_status", q1)

    def test_drift_monitoring_png_exists(self):
        """Drift monitoring visualization must be rendered."""
        self.assertTrue(os.path.exists(self.drift_png), f"Missing {self.drift_png}")
        self.assertGreater(os.path.getsize(self.drift_png), 50000, "Drift chart file too small")


if __name__ == "__main__":
    unittest.main()
