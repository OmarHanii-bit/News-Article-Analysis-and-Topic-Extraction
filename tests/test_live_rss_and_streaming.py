"""
Unit & Regression Tests for Live RSS & Streaming Pipeline
=========================================================
Author: Yasmine Mohamed (PySpark Distributed Computing Engineer)
Collaborators: Omar Hani, Mohammed Hossam
"""

import os
import json
import unittest
import pandas as pd


class TestLiveRSSAndStreaming(unittest.TestCase):
    """Validates live RSS feeds and streaming pipeline outputs."""

    def test_live_rss_dataset_exists(self):
        """Live RSS CSV must exist, have valid rows and authentic publishers."""
        csv_path = "data/real_news/live_rss_articles.csv"
        prov_path = "data/real_news/live_rss_provenance.json"

        self.assertTrue(os.path.exists(csv_path), f"Missing {csv_path}")
        df = pd.read_csv(csv_path)
        self.assertGreater(len(df), 50, "Should have ingested at least 50 live articles")
        self.assertIn("The New York Times", df["publication"].values)
        self.assertIn("BBC News", df["publication"].values)

        self.assertTrue(os.path.exists(prov_path), f"Missing {prov_path}")
        with open(prov_path, "r", encoding="utf-8") as f:
            prov = json.load(f)
        self.assertFalse(prov["date_derived"], "Live RSS dates must not be derived")

    def test_streaming_parquet_sink_exists(self):
        """Streaming Parquet directory must contain committed micro-batches."""
        sink_dir = "data/streaming_parquet"
        if not os.path.isdir(sink_dir):
            self.skipTest("Streaming sink not built yet (run `python run_all.py` or the streaming stage).")
        parquet_files = [f for f in os.listdir(sink_dir) if f.endswith(".parquet")]
        self.assertGreater(len(parquet_files), 0, "No Parquet files found in streaming sink")


if __name__ == "__main__":
    unittest.main()
