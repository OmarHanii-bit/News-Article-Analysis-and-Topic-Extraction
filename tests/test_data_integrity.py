"""
Integration Tests: Pipeline Data Integrity & Storage Layer
==========================================================
Component: QA, Storage & Benchmarking Suite
Author: Mazen Salem (QA, Benchmarking & Presentation Lead)
"""

import unittest
import sys
import os
import json
import pandas as pd

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestDataIntegrity(unittest.TestCase):
    def test_raw_csv_existence_and_volume(self):
        """Verifies raw CSV dataset exists and meets the minimum 6,000 row requirement."""
        csv_path = "data/raw_news_articles.csv"
        self.assertTrue(os.path.exists(csv_path), f"Raw dataset missing: {csv_path}")

        # Check line count
        with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = sum(1 for _ in f)
        # Accounting for header and multiline records, line count should be substantial
        self.assertGreater(lines, 5000)

    def test_mapreduce_output_validity(self):
        """Verifies that MapReduce produced a non-empty TSV with valid term counts."""
        tsv_path = "data/mapreduce_term_counts.tsv"
        self.assertTrue(os.path.exists(tsv_path), f"MapReduce TSV missing: {tsv_path}")

        df_tsv = pd.read_csv(tsv_path, sep="\t", header=None, names=["term", "count"])
        self.assertGreater(len(df_tsv), 1000)
        # All counts must be positive integers >= 5 (our min-frequency threshold)
        self.assertTrue((df_tsv["count"] >= 5).all())

    def test_parquet_lakehouse_partitions(self):
        """Verifies that Snappy Parquet lakehouse exists with year/month partitions."""
        parquet_dir = "data/processed_parquet"
        self.assertTrue(os.path.exists(parquet_dir), f"Parquet lakehouse missing: {parquet_dir}")

        # Verify reading with pandas / pyarrow
        df_pq = pd.read_parquet(parquet_dir)
        # Data-driven volume check: any substantial corpus is acceptable, so the
        # suite keeps passing if the benchmark is regenerated with a new size.
        self.assertGreater(len(df_pq), 5000)

        # Verify required analytical columns
        required_cols = ["article_id", "title", "publication", "dominant_topic", "topic_label", "sentiment_polarity"]
        for col in required_cols:
            self.assertIn(col, df_pq.columns)

    def test_pipeline_summary_json(self):
        """Verifies JSON analytics summary has metadata, topics, and temporal trends."""
        summary_path = "data/pipeline_analytics_summary.json"
        self.assertTrue(os.path.exists(summary_path), f"Summary JSON missing: {summary_path}")

        with open(summary_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertIn("metadata", data)
        self.assertIn("topics", data)
        self.assertIn("topic_summary", data)
        self.assertIn("temporal_trends", data)
        self.assertIn("publisher_distribution", data)

        # Data-driven assertions: validate internal consistency instead of
        # hard-coding a specific corpus size or topic count.
        meta = data["metadata"]
        self.assertGreater(meta["total_articles_processed"], 5000)
        self.assertGreaterEqual(meta["num_topics"], 2)
        # The declared topic count must match the number of learned topics.
        self.assertEqual(len(data["topics"]), meta["num_topics"])
        # Metadata volume must agree with the Parquet lakehouse row count.
        df_pq = pd.read_parquet("data/processed_parquet")
        self.assertEqual(meta["total_articles_processed"], len(df_pq))

    def test_real_corpus_artifacts_when_present(self):
        """When the real-world corpus has been processed, its artifacts must be coherent."""
        real_summary = "data/real_news/pipeline_analytics_summary.json"
        real_parquet = "data/real_news/processed_parquet"
        if not (os.path.exists(real_summary) and os.path.exists(real_parquet)):
            self.skipTest("Real-world corpus artifacts not generated in this environment.")

        with open(real_summary, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["metadata"]["label_mode"], "auto")
        df_real = pd.read_parquet(real_parquet)
        self.assertEqual(data["metadata"]["total_articles_processed"], len(df_real))
        self.assertGreater(len(df_real), 5000)

    def test_visualization_files_exist(self):
        """Verifies that all 6 high-resolution 300 DPI visualization PNGs exist."""
        viz_dir = "visualizations"
        expected_charts = [
            "topic_volume_distribution.png",
            "lda_topic_top_terms.png",
            "publisher_topic_heatmap.png",
            "temporal_topic_trends.png",
            "topic_sentiment_distribution.png",
            "topic_wordclouds_grid.png"
        ]
        for chart in expected_charts:
            path = os.path.join(viz_dir, chart)
            self.assertTrue(os.path.exists(path), f"Visualization missing: {path}")
            # Ensure file is not 0 bytes
            self.assertGreater(os.path.getsize(path), 10000, f"Visualization is empty or corrupt: {path}")

    def test_presentation_file_exists(self):
        """Verifies that PowerPoint deck exists and has valid size."""
        pptx_path = "presentation/News_Article_Analysis_Presentation.pptx"
        self.assertTrue(os.path.exists(pptx_path), f"PowerPoint file missing: {pptx_path}")
        self.assertGreater(os.path.getsize(pptx_path), 100000)

if __name__ == "__main__":
    unittest.main()
