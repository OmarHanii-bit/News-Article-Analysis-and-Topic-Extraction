"""
Unit Tests: PySpark Schema Enforcement & Sentiment UDF
======================================================
Component: PySpark Distributed Pipeline Testing Suite
Author: Marcus Chen (PySpark Distributed Engineer)
"""

import unittest
import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.spark_pipeline import define_explicit_schema, compute_vader_sentiment, extract_dominant_topic

class TestSparkPipelineLogic(unittest.TestCase):
    def test_schema_definition(self):
        """Verifies StructType schema fields and types."""
        schema = define_explicit_schema()
        field_names = [f.name for f in schema.fields]
        expected_fields = [
            "article_id", "title", "author", "date", "year", "month",
            "publication", "category", "sub_topic", "content", "word_count", "url"
        ]
        self.assertEqual(len(field_names), 12)
        for expected in expected_fields:
            self.assertIn(expected, field_names)

    def test_sentiment_scoring_bounds(self):
        """Verifies that VADER compound sentiment stays within [-1.0, +1.0]."""
        pos_text = "This is a great, wonderful victory with excellent success and happy results."
        neg_text = "This is a terrible, horrible disaster with painful tragedy and catastrophic failure."
        neutral_text = "The committee convened at 14:00 GMT to review the documentation."

        score_pos = compute_vader_sentiment(pos_text)
        score_neg = compute_vader_sentiment(neg_text)
        score_neu = compute_vader_sentiment(neutral_text)

        self.assertGreater(score_pos, 0.4)
        self.assertLess(score_neg, -0.4)
        self.assertGreaterEqual(score_pos, -1.0)
        self.assertLessEqual(score_pos, 1.0)
        self.assertGreaterEqual(score_neg, -1.0)
        self.assertLessEqual(score_neg, 1.0)
        self.assertAlmostEqual(score_neu, 0.0, delta=0.2)

    def test_sentiment_null_handling(self):
        """Verifies graceful handling of empty or None inputs."""
        self.assertEqual(compute_vader_sentiment(None), 0.0)
        self.assertEqual(compute_vader_sentiment(""), 0.0)

    def test_dominant_topic_extraction(self):
        """Verifies extraction of highest probability topic index."""
        distribution = [0.05, 0.10, 0.70, 0.05, 0.05, 0.05]
        dominant = extract_dominant_topic(distribution)
        self.assertEqual(dominant, 2)

        null_dominant = extract_dominant_topic(None)
        self.assertEqual(null_dominant, -1)

if __name__ == "__main__":
    unittest.main()
