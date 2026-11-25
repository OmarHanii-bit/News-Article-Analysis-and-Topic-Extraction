"""
Verification Tests: Interactive Dashboards
==========================================
Component: Visualization & UI Regression Suite
Author: Mazen Salem (QA, Benchmarking & Presentation Lead)
         Mohamed Magdi (Visualization, Streamlit & Offline HTML Lead)

Covers the live Streamlit application (src/dashboard.py) and the corpus-aware
offline Plotly HTML dashboard builder (src/build_interactive_dashboard.py).
"""

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.build_interactive_dashboard import CORPORA, build_ecosystem_panel, build_figures

SYNTHETIC_SUMMARY = os.path.join(ROOT, "data", "pipeline_analytics_summary.json")
REAL_SUMMARY = os.path.join(ROOT, "data", "real_news", "pipeline_analytics_summary.json")
ECO_SUMMARY = os.path.join(ROOT, "data", "hadoop_ecosystem", "hadoop_ecosystem_summary.json")


class TestOfflineDashboardBuilder(unittest.TestCase):
    """Contracts of the corpus-aware offline HTML dashboard builder."""

    def setUp(self):
        # The builder resolves its corpus paths relative to the project root.
        self._cwd = os.getcwd()
        os.chdir(ROOT)

    def tearDown(self):
        os.chdir(self._cwd)

    def test_registry_covers_both_corpora(self):
        """The builder must mirror the dashboard's dual-corpus registry."""
        self.assertEqual(set(CORPORA.keys()), {"synthetic", "real"})

    def test_corpora_map_to_distinct_artifacts(self):
        """Each corpus must resolve to its own Parquet lake, summary and HTML file."""
        for field in ("parquet", "summary", "output"):
            values = {cfg[field] for cfg in CORPORA.values()}
            self.assertEqual(len(values), 2, f"corpora share the same '{field}' path")

    @unittest.skipUnless(os.path.exists(SYNTHETIC_SUMMARY), "synthetic analytics summary absent")
    def test_builder_emits_seven_figures_for_synthetic(self):
        """The builder renders the full seven-chart analytics suite."""
        frame, summary, figures = build_figures(
            CORPORA["synthetic"]["parquet"], CORPORA["synthetic"]["summary"])
        self.assertEqual(len(figures), 7)
        self.assertEqual(summary["metadata"]["label_mode"], "curated")
        self.assertIn("topic_label", frame.columns)
        self.assertIn("sentiment_polarity", frame.columns)

    @unittest.skipUnless(os.path.exists(REAL_SUMMARY), "real-corpus analytics summary absent")
    def test_builder_handles_noisy_real_corpus(self):
        """The real Reuters corpus (90 categories, 1 publisher) must render without special-casing."""
        frame, summary, figures = build_figures(
            CORPORA["real"]["parquet"], CORPORA["real"]["summary"])
        self.assertEqual(len(figures), 7)
        self.assertEqual(summary["metadata"]["label_mode"], "auto")
        self.assertGreater(len(frame), 10000)

    @unittest.skipUnless(os.path.exists(ECO_SUMMARY), "Hadoop ecosystem summary absent")
    def test_ecosystem_panel_publishes_hdfs_invariant(self):
        """The corpus-independent stack panel must be produced from the ecosystem evidence."""
        cards, table = build_ecosystem_panel()
        self.assertIsNotNone(cards)
        self.assertIsNotNone(table)
        self.assertIn("Replication Factor", cards)
        services = list(table.data[0].cells.values[0])
        self.assertIn("HDFS", services)
        self.assertIn("Oozie", services)


class TestOfflineDashboardArtifacts(unittest.TestCase):
    """The generated HTML artifacts must be self-contained and corpus-specific."""

    def _read(self, key):
        path = os.path.join(ROOT, CORPORA[key]["output"])
        if not os.path.exists(path):
            self.skipTest(f"{path} not built yet")
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()

    def test_synthetic_html_is_self_contained(self):
        """The offline dashboard must inline Plotly so it works with no server or internet."""
        html = self._read("synthetic")
        self.assertIn("plotly", html.lower())
        self.assertIn("Hadoop Ecosystem Stack Execution Evidence", html)
        self.assertIn("Synthetic Benchmark", html)

    def test_real_html_is_corpus_specific(self):
        """The real-corpus dashboard must not silently fall back to synthetic data."""
        html = self._read("real")
        self.assertIn("Reuters", html)
        self.assertNotIn("Synthetic Benchmark", html)

    def test_both_html_artifacts_publish_decision_panel(self):
        """Both corpora must surface the Key Takeaways & Decisions / What next panel."""
        for key in ("synthetic", "real"):
            with self.subTest(corpus=key):
                html = self._read(key)
                self.assertIn("Key Takeaways & Decisions", html)
                self.assertIn("What next", html)
                self.assertIn("Metric / numeric trigger", html)
                self.assertIn("Owner", html)
                self.assertIn("Horizon", html)

    def test_both_html_titles_name_the_active_corpus(self):
        """The two dashboard files must be distinguishable from their <title> alone."""
        for key in ("synthetic", "real"):
            with self.subTest(corpus=key):
                html = self._read(key)
                title = html.split("<title>", 1)[1].split("</title>", 1)[0]
                self.assertIn(CORPORA[key]["label"], title)
        self.assertNotIn(
            self._read("synthetic").split("<title>", 1)[1].split("</title>", 1)[0],
            self._read("real").split("<title>", 1)[1].split("</title>", 1)[0],
        )

    def test_both_html_artifacts_carry_sampling_and_confidence_captions(self):
        """Sampling and confidence-calibration caveats must travel with the artifacts."""
        for key in ("synthetic", "real"):
            with self.subTest(corpus=key):
                html = self._read(key)
                self.assertIn("n=1,500 random sample (seed 42)", html)
                self.assertIn("0.991–0.993", html)
                self.assertIn("0.684–0.892", html)


class TestStreamlitApplication(unittest.TestCase):
    """Headless smoke test of the live Streamlit application."""

    def setUp(self):
        self._cwd = os.getcwd()
        os.chdir(ROOT)

    def tearDown(self):
        os.chdir(self._cwd)

    def test_app_renders_default_view_without_exception(self):
        """The default Intelligence Command Center must execute with no runtime error."""
        try:
            from streamlit.testing.v1 import AppTest
        except ImportError:
            self.skipTest("streamlit.testing is unavailable")

        app = AppTest.from_file(os.path.join(ROOT, "src", "dashboard.py"), default_timeout=300)
        app.run()
        self.assertEqual(len(app.exception), 0, msg=[str(e.value) for e in app.exception])
        self.assertGreaterEqual(len(app.title), 1)
        self.assertGreaterEqual(len(app.metric), 4)

        # The Intelligence Command Center is the default landing view; every
        # visible article card must carry a real brief summary, not the old
        # "No summary available." placeholder caused by a field-name mismatch.
        summaries = [m.value for m in app.markdown if "Brief Summary" in m.value]
        self.assertGreaterEqual(len(summaries), 12)
        for block in summaries:
            self.assertNotIn("No summary available", block)
            self.assertNotIn("Click here for full terms", block)


if __name__ == "__main__":
    unittest.main(verbosity=2)
