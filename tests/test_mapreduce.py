"""
Unit Tests: Hadoop MapReduce Tokenization & Reduction Logic
===========================================================
Component: Hadoop Ecosystem Testing Suite
Author: Priya Sharma (Hadoop & MapReduce Specialist)
"""

import unittest
import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.mapreduce.mapper import REGEX_CLEAN_HTML, REGEX_CLEAN_URL, REGEX_CLEAN_ENTITIES, REGEX_WORD_TOKENS, STOPWORDS

class TestMapReduceLogic(unittest.TestCase):
    def test_html_stripping(self):
        """Verifies that HTML tags are purged cleanly."""
        raw_text = "<p>Breaking news in <b>technology</b>.</p><br/>Read more."
        cleaned = REGEX_CLEAN_HTML.sub(" ", raw_text)
        self.assertNotIn("<p>", cleaned)
        self.assertNotIn("<b>", cleaned)
        self.assertNotIn("</b>", cleaned)
        self.assertNotIn("<br/>", cleaned)
        self.assertIn("Breaking", cleaned)

    def test_url_stripping(self):
        """Verifies that web URLs are purged cleanly."""
        raw_text = "Visit https://www.reuters.com/news/101 for live updates."
        cleaned = REGEX_CLEAN_URL.sub(" ", raw_text)
        self.assertNotIn("https://", cleaned)
        self.assertIn("Visit", cleaned)

    def test_entity_stripping(self):
        """Verifies that HTML entities like &amp; are purged."""
        raw_text = "Standard &amp; Poors &quot;Special Report&quot;."
        cleaned = REGEX_CLEAN_ENTITIES.sub(" ", raw_text)
        self.assertNotIn("&amp;", cleaned)
        self.assertNotIn("&quot;", cleaned)

    def test_token_extraction(self):
        """Verifies regex extraction of valid alphabetic tokens."""
        text = "artificial intelligence clusters achieved breakthrough results in 2026"
        tokens = REGEX_WORD_TOKENS.findall(text.lower())
        self.assertIn("artificial", tokens)
        self.assertIn("intelligence", tokens)
        self.assertIn("breakthrough", tokens)
        # Numbers like 2026 should not be tokenized as alphabetic words
        self.assertNotIn("2026", tokens)

    def test_stopword_filtering(self):
        """Verifies that non-informative stopwords are discarded."""
        tokens = ["the", "cloud", "and", "quantum", "computing", "with", "reuters"]
        filtered = [t for t in tokens if t not in STOPWORDS]
        self.assertNotIn("the", filtered)
        self.assertNotIn("and", filtered)
        self.assertNotIn("with", filtered)
        self.assertNotIn("reuters", filtered)
        self.assertIn("cloud", filtered)
        self.assertIn("quantum", filtered)
        self.assertIn("computing", filtered)

    def test_bigram_generation(self):
        """Verifies bigram creation logic."""
        tokens = ["quantum", "coherence", "qubit"]
        bigrams = [f"BI_{tokens[i]}_{tokens[i+1]}" for i in range(len(tokens) - 1)]
        self.assertEqual(len(bigrams), 2)
        self.assertEqual(bigrams[0], "BI_quantum_coherence")
        self.assertEqual(bigrams[1], "BI_coherence_qubit")

if __name__ == "__main__":
    unittest.main()
