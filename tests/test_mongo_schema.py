"""
Unit Tests: MongoDB NoSQL Document Modeling & BSON Schema
=========================================================
Component: MongoDB NoSQL Database Testing Suite
Author: Mohamed Ibrahim (NoSQL & MongoDB Specialist)
"""

import unittest
import sys
import os
import json

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestMongoSchema(unittest.TestCase):
    def setUp(self):
        self.offline_dir = "data/mongodb_offline_collections"

    def test_topic_definitions_collection(self):
        """Verifies structure and integrity of topic definitions documents."""
        topic_file = os.path.join(self.offline_dir, "topic_definitions.json")
        self.assertTrue(os.path.exists(topic_file), f"Missing: {topic_file}")

        with open(topic_file, "r", encoding="utf-8") as f:
            topics = json.load(f)

        self.assertGreaterEqual(len(topics), 5)
        for doc in topics:
            self.assertIn("topic_key", doc)
            self.assertIn("topic_id", doc)
            self.assertIn("terms", doc)
            self.assertIn("weights", doc)
            self.assertIn("label_summary", doc)
            self.assertIsInstance(doc["terms"], list)
            self.assertIsInstance(doc["weights"], list)
            self.assertEqual(len(doc["terms"]), len(doc["weights"]))

    def test_publisher_analytics_collection(self):
        """Verifies structure and non-emptiness of publisher analytics documents."""
        pub_file = os.path.join(self.offline_dir, "publisher_analytics.json")
        self.assertTrue(os.path.exists(pub_file), f"Missing: {pub_file}")

        with open(pub_file, "r", encoding="utf-8") as f:
            pub_docs = json.load(f)

        self.assertGreater(len(pub_docs), 0)
        sample = pub_docs[0]
        self.assertIn("publication", sample)
        self.assertIn("topic_label", sample)
        self.assertIn("count", sample)

    def test_articles_sample_collection(self):
        """Verifies enriched article documents contain ML inference fields."""
        art_file = os.path.join(self.offline_dir, "articles_sample.json")
        self.assertTrue(os.path.exists(art_file), f"Missing: {art_file}")

        with open(art_file, "r", encoding="utf-8") as f:
            articles = json.load(f)

        self.assertGreater(len(articles), 0)
        sample = articles[0]
        # Must contain ML enriched columns
        self.assertIn("article_id", sample)
        self.assertIn("dominant_topic", sample)
        self.assertIn("topic_label", sample)
        self.assertIn("sentiment_polarity", sample)
        self.assertIn("topic_confidence", sample)

if __name__ == "__main__":
    unittest.main()
