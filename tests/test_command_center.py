"""
Verification Tests: Intelligence Command Center
================================================
Component: Static dashboard payload and artifact contract.

Covers the generator in ``src/build_command_center.py`` and the committed
``dashboard/command_center.html``. The generator is exercised in memory; the
artifact test only reads the file, so the suite stays fast and offline.
"""

import json
import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.build_command_center import brief_summary, build_payload

DASHBOARD = os.path.join(ROOT, "dashboard", "command_center.html")


def load_embedded_payload(path=DASHBOARD):
    with open(path, "r", encoding="utf-8") as handle:
        html = handle.read()
    match = re.search(
        r'<script id="payload" type="application/json">(.*?)</script>',
        html, re.S)
    if not match:
        raise AssertionError("payload JSON island not found")
    return html, json.loads(match.group(1).replace("<\\/", "</"))


class TestCommandCenterPayload(unittest.TestCase):
    """The generated payload must be internally consistent and filterable."""

    @classmethod
    def setUpClass(cls):
        cls.payload = build_payload()

    def test_topics_cover_both_corpora_and_sum_to_total(self):
        """Every topic belongs to a corpus and volumes reconcile to the KPI."""
        topics = self.payload["topics"]
        self.assertGreaterEqual(len(topics), 8)
        self.assertEqual({t["corpus"] for t in topics}, {"synthetic", "reuters"})
        self.assertEqual(sum(t["volume"] for t in topics),
                         self.payload["kpis"]["total_articles"])
        ids = [t["id"] for t in topics]
        self.assertEqual(len(ids), len(set(ids)), "topic ids must be unique")

    def test_articles_support_every_filter_dimension(self):
        """The feed needs enough rows, dates, sources and topics to cross-filter."""
        articles = self.payload["articles"]
        self.assertGreaterEqual(len(articles), 12)
        self.assertGreaterEqual(len({a["topic"] for a in articles}), 8)
        self.assertGreaterEqual(len({a["source"] for a in articles}), 2)
        topic_ids = {t["id"] for t in self.payload["topics"]}
        self.assertTrue({a["topic"] for a in articles} <= topic_ids)
        self.assertTrue(all(a["date"] for a in articles))
        self.assertLessEqual(max(a["date"] for a in articles),
                             self.payload["kpis"]["date_max"])

        # The Streamlit ICC shows brief_summary() on every card, so each row
        # must yield a clean digest: no placeholder, no raw markup or
        # syndication boilerplate, and never longer than the display cap.
        digests = [brief_summary(a) for a in articles]
        self.assertTrue(all(d and d != "No summary available." for d in digests))
        self.assertFalse(any("Click here for full terms" in d or "<br" in d
                             for d in digests))
        self.assertLessEqual(max(len(d) for d in digests), 200)

    def test_entities_are_typed_and_sentiment_buckets_reconcile(self):
        """Entity counts must equal the sum of their sentiment buckets."""
        entities = self.payload["entities"]
        self.assertGreaterEqual(len(entities), 12)
        self.assertTrue({e["type"] for e in entities} <= {"ORG", "PERSON", "LOC"})
        for entity in entities:
            self.assertEqual(
                entity["count"],
                entity["positive"] + entity["neutral"] + entity["negative"],
                entity["name"])
            self.assertGreater(entity["count"], 0)

    def test_monthly_timeline_is_complete(self):
        """The 24-month timeline must carry every topic's monthly series."""
        months = self.payload["months"]
        self.assertEqual(len(months), 24)
        self.assertEqual(months[0], "2024-01")
        self.assertEqual(months[-1], "2025-12")
        for month in months:
            bucket = self.payload["timeline"][month]
            self.assertIn("total", bucket)
            self.assertIn("by_topic", bucket)
        self.assertEqual(sum(self.payload["timeline"][m]["total"] for m in months),
                         self.payload["kpis"]["total_articles"])


class TestCommittedCommandCenter(unittest.TestCase):
    """The committed single-file dashboard must be self-contained."""

    @unittest.skipUnless(os.path.exists(DASHBOARD), "command center artifact absent")
    def test_artifact_contains_every_required_panel(self):
        html, payload = load_embedded_payload()
        for element_id in (
            "searchInput", "dateRange", "sourceBtn", "clusterMap", "timeline",
            "entityList", "feed", "drawer", "sentiment",
        ):
            if element_id == "sentiment":
                self.assertIn("Net Sentiment Index", html)
                continue
            self.assertIn(f'id="{element_id}"', html, element_id)
        self.assertGreaterEqual(len(payload["topics"]), 8)
        self.assertGreaterEqual(len(payload["articles"]), 12)

    @unittest.skipUnless(os.path.exists(DASHBOARD), "command center artifact absent")
    def test_artifact_has_no_external_script_dependency(self):
        """The dashboard must render offline: no CDN scripts, no fetch calls."""
        html, _payload = load_embedded_payload()
        self.assertNotRegex(html, r"<script[^>]+src=")
        self.assertNotIn("plotly.js", html)
        self.assertNotIn("cdn.jsdelivr.net", html)
        self.assertNotIn("unpkg.com", html)


if __name__ == "__main__":
    unittest.main()
