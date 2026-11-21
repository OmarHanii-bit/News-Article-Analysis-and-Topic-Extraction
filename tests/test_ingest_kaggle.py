"""
Unit Tests: Kaggle 'All the News' Ingestion Adapter
===================================================
Component: External Dataset Ingestion & Schema Adaptation Suite
Author: Alex Vance (Project Lead & Solutions Architect)
        David Kim (QA, Benchmarking & Presentation Lead)

Covers src/ingest_kaggle_real.py, the chunked adapter that normalizes
heterogeneous Kaggle news CSV schema variants into the canonical 12-attribute
pipeline contract consumed by PySpark, MapReduce and MongoDB.
"""

import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pandas as pd

from src.ingest_kaggle_real import TARGET_COLUMNS, ingest_kaggle_file, map_kaggle_row_chunk

# A 21-word article body comfortably clears the >= 15 word hygiene threshold.
VALID_BODY = (
    "The central bank held interest rates steady on Thursday citing cooling "
    "inflation and a resilient labour market across the euro area"
)


def build_chunk(n_rows=3, body=VALID_BODY):
    """Builds a Kaggle-style chunk using the alias column names, not canonical ones."""
    return pd.DataFrame(
        {
            "headline": [f"Headline {i}" for i in range(n_rows)],
            "article": [body for _ in range(n_rows)],
            "byline": ["Jane Doe"] * n_rows,
            "published_at": ["2021-03-04"] * n_rows,
            "publisher": ["Global Wire"] * n_rows,
            "section": ["Economy"] * n_rows,
            "link": [f"https://example.com/story/{i}" for i in range(n_rows)],
        }
    )


class TestKaggleColumnMapping(unittest.TestCase):
    """Polymorphic column mapping and data-hygiene contracts."""

    def test_alias_columns_map_to_canonical_schema(self):
        """Alternative Kaggle column names must land in the canonical 12-attribute contract."""
        mapped = map_kaggle_row_chunk(build_chunk(), 0)
        self.assertEqual(list(mapped.columns), TARGET_COLUMNS)
        self.assertEqual(mapped.loc[0, "title"], "Headline 0")
        self.assertEqual(mapped.loc[0, "author"], "Jane Doe")
        self.assertEqual(mapped.loc[0, "content"], VALID_BODY)
        self.assertEqual(mapped.loc[0, "publication"], "Global Wire")
        self.assertEqual(mapped.loc[0, "category"], "Economy")
        self.assertEqual(mapped.loc[0, "url"], "https://example.com/story/0")

    def test_temporal_decomposition(self):
        """Dates must be normalized to ISO format and decomposed for Parquet partitioning."""
        mapped = map_kaggle_row_chunk(build_chunk(), 0)
        self.assertEqual(mapped.loc[0, "date"], "2021-03-04")
        self.assertEqual(mapped.loc[0, "year"], 2021)
        self.assertEqual(mapped.loc[0, "month"], 3)
        # year/month must be integral so the Parquet partition keys stay year=YYYY/month=MM
        self.assertEqual(int(mapped.loc[0, "year"]), 2021)
        self.assertEqual(int(mapped.loc[0, "month"]), 3)

    def test_word_count_is_computed(self):
        """Word counts drive the short-article hygiene filter and downstream statistics."""
        mapped = map_kaggle_row_chunk(build_chunk(), 0)
        self.assertEqual(mapped.loc[0, "word_count"], len(VALID_BODY.split()))

    def test_identifier_prefix_uses_existing_id_column(self):
        """An explicit identifier column must be namespaced with the KAG_ prefix."""
        chunk = build_chunk(n_rows=1)
        chunk["id"] = ["42"]
        mapped = map_kaggle_row_chunk(chunk, 0)
        self.assertEqual(mapped.loc[0, "article_id"], "KAG_42")

    def test_identifier_fallback_is_chunk_positional(self):
        """Without an identifier column, synthetic ids must encode chunk index and row offset."""
        mapped = map_kaggle_row_chunk(build_chunk(n_rows=2), 3)
        self.assertEqual(mapped.loc[0, "article_id"], "KAG_3_000000")
        self.assertEqual(mapped.loc[1, "article_id"], "KAG_3_000001")

    def test_missing_content_column_raises(self):
        """Full article text is mandatory — a headline-only file must be rejected."""
        chunk = build_chunk(n_rows=1).drop(columns=["article"])
        with self.assertRaises(ValueError):
            map_kaggle_row_chunk(chunk, 0)

    def test_short_articles_are_filtered_out(self):
        """Rows below the 15-word hygiene threshold must be purged."""
        mapped = map_kaggle_row_chunk(build_chunk(n_rows=2, body="Too short."), 0)
        self.assertTrue(mapped.empty)
        self.assertEqual(list(mapped.columns), TARGET_COLUMNS)

    def test_missing_optional_fields_use_documented_defaults(self):
        """A minimal content-only chunk must still satisfy every schema contract."""
        chunk = pd.DataFrame({"text": [VALID_BODY]})
        mapped = map_kaggle_row_chunk(chunk, 0)
        self.assertEqual(mapped.loc[0, "title"], "Untitled Article")
        self.assertEqual(mapped.loc[0, "author"], "Staff Correspondent")
        self.assertEqual(mapped.loc[0, "publication"], "Global Wire")
        self.assertEqual(mapped.loc[0, "category"], "General")
        self.assertEqual(mapped.loc[0, "sub_topic"], "General - General Beat")
        self.assertEqual(mapped.loc[0, "date"], "2024-06-15")
        self.assertEqual(mapped.loc[0, "year"], 2024)
        self.assertEqual(mapped.loc[0, "month"], 6)
        self.assertTrue(mapped.loc[0, "url"].startswith("https://www.news-wire.com/article/"))

    def test_unparseable_date_falls_back_to_default(self):
        """Corrupt/absent timestamps must degrade gracefully, never crash the chunk."""
        chunk = build_chunk(n_rows=1)
        chunk["published_at"] = ["not-a-date"]
        mapped = map_kaggle_row_chunk(chunk, 0)
        self.assertEqual(mapped.loc[0, "date"], "2024-06-15")
        self.assertEqual(mapped.loc[0, "year"], 2024)


class TestKaggleFileIngestion(unittest.TestCase):
    """Chunked streaming ingestion against real (small) CSV files on disk."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = self._tmp.name
        self.input_csv = os.path.join(self.tmp_dir, "all-the-news.csv")
        self.output_csv = os.path.join(self.tmp_dir, "out", "raw_news_articles.csv")

    def tearDown(self):
        self._tmp.cleanup()

    def test_missing_input_file_returns_false(self):
        """A missing source file must report failure instead of raising."""
        self.assertFalse(ingest_kaggle_file(os.path.join(self.tmp_dir, "nope.csv"), self.output_csv))

    def test_chunked_ingestion_writes_canonical_csv(self):
        """Streaming must concatenate chunks into one canonical, header-correct CSV."""
        build_chunk(n_rows=12).to_csv(self.input_csv, index=False)
        self.assertTrue(ingest_kaggle_file(self.input_csv, self.output_csv, chunk_size=5))
        written = pd.read_csv(self.output_csv)
        self.assertEqual(list(written.columns), TARGET_COLUMNS)
        self.assertEqual(len(written), 12)

    def test_max_records_limit_is_respected(self):
        """The record cap must truncate mid-chunk and stop reading further chunks."""
        build_chunk(n_rows=12).to_csv(self.input_csv, index=False)
        self.assertTrue(ingest_kaggle_file(self.input_csv, self.output_csv, chunk_size=5, max_records=6))
        self.assertEqual(len(pd.read_csv(self.output_csv)), 6)

    def test_noisy_rows_never_reach_the_output(self):
        """Short bodies must be dropped while valid rows in the same chunk survive."""
        noisy = pd.concat([build_chunk(n_rows=3), build_chunk(n_rows=2, body="Too short.")], ignore_index=True)
        noisy.to_csv(self.input_csv, index=False)
        self.assertTrue(ingest_kaggle_file(self.input_csv, self.output_csv, chunk_size=10))
        written = pd.read_csv(self.output_csv)
        self.assertEqual(len(written), 3)
        self.assertTrue((written["word_count"] >= 15).all())


class TestKaggleCliContract(unittest.TestCase):
    """The adapter's documented command line must execute end to end."""

    def test_documented_cli_flags_are_accepted(self):
        """README Method 7 documents --chunk-size / --max-rows; both must be honoured."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_csv = os.path.join(tmp_dir, "all-the-news.csv")
            output_csv = os.path.join(tmp_dir, "out", "raw.csv")
            build_chunk(n_rows=10).to_csv(input_csv, index=False)

            result = subprocess.run(
                [
                    sys.executable, os.path.join("src", "ingest_kaggle_real.py"),
                    "--input", input_csv,
                    "--output", output_csv,
                    "--chunk-size", "4",
                    "--max-rows", "6",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                errors="replace",
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(os.path.exists(output_csv))
            self.assertEqual(len(pd.read_csv(output_csv)), 6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
