"""
Unit & Regression Tests for Supervised Classification
=====================================================
Author: Shehab Osama (NLP & Machine Learning Scientist)
Collaborator: Mazen Salem (QA, Benchmarking & Presentation Lead)
"""

import os
import json
import unittest


class TestSupervisedClassifier(unittest.TestCase):
    """Validates supervised classification outputs and performance metrics."""

    def setUp(self):
        self.metrics_path = "data/supervised_classification_metrics.json"
        self.cm_path = "visualizations/supervised_confusion_matrix.png"
        self.comp_path = "visualizations/supervised_model_comparison.png"

    def test_metrics_file_exists_and_valid(self):
        """Metrics payload must exist and contain required benchmark keys."""
        self.assertTrue(os.path.exists(self.metrics_path), f"Missing {self.metrics_path}")
        with open(self.metrics_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertIn("best_model", data)
        self.assertIn("best_test_macro_f1", data)
        self.assertIn("models_benchmarked", data)
        self.assertGreater(data["best_test_macro_f1"], 0.75, "Best model Macro-F1 should exceed 0.75")
        self.assertGreaterEqual(data["total_articles_evaluated"], 9000)

    def test_confusion_matrix_png_exists(self):
        """Publication-grade confusion matrix PNG must be generated."""
        self.assertTrue(os.path.exists(self.cm_path), f"Missing {self.cm_path}")
        self.assertGreater(os.path.getsize(self.cm_path), 50000, "Confusion matrix file too small")

    def test_model_comparison_png_exists(self):
        """Model benchmark comparison chart must be generated."""
        self.assertTrue(os.path.exists(self.comp_path), f"Missing {self.comp_path}")
        self.assertGreater(os.path.getsize(self.comp_path), 50000, "Comparison chart too small")


if __name__ == "__main__":
    unittest.main()
