"""
Unit Tests: Hadoop MapReduce Tokenization & Reduction Logic
===========================================================
Component: Hadoop Ecosystem Testing Suite
Author: Priya Sharma (Hadoop & MapReduce Specialist)
"""

import unittest
import sys
import os
import io

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.mapreduce.mapper as mapper_module
import src.mapreduce.reducer as reducer_module
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

    def test_numeric_and_hash_entities_stripped(self):
        """Numeric ('&#39;') and '#' entities must be purged like Spark does."""
        raw = "Caf&#39;s price is &lt;10&gt; &amp; rising &#x26; more."
        cleaned = REGEX_CLEAN_ENTITIES.sub(" ", raw)
        for entity in ("&#39;", "&lt;", "&gt;", "&amp;", "&#x26;"):
            self.assertNotIn(entity, cleaned, f"Entity not stripped: {entity}")
        # The mapper regex must stay byte-for-byte aligned with the Spark one.
        self.assertEqual(REGEX_CLEAN_ENTITIES.pattern, r"&[a-z0-9#]+;")

    def test_mapper_process_stream_emits_unigrams_and_bigrams(self):
        """Exercises the mapper's real stdin->stdout streaming path."""
        csv_text = (
            "article_id,title,author,date,year,month,publication,category,sub_topic,content,word_count,url\n"
            "A1,Quantum &amp; coherence,Reporter,2025-05-01,2025,5,Press,Science,Physics,"
            "\"Markets rally &gt; expectations today\",100,https://example.com\n"
        )
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdin, sys.stdout = io.StringIO(csv_text), io.StringIO()
        try:
            mapper_module.process_stream()
            emitted = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_stdout
        self.assertIn("quantum\t1", emitted)
        self.assertIn("coherence\t1", emitted)
        self.assertIn("BI_quantum_coherence", emitted)
        self.assertNotIn("amp\t1", emitted)   # entity text never becomes a term
        self.assertNotIn("gt\t1", emitted)
        self.assertNotIn("https", emitted)    # URL stripped before tokenization

    def test_mapper_numeric_entities_do_not_leak_into_stream(self):
        """A numeric entity must not create junk unigrams in the mapper output."""
        csv_text = "id,title\nA2,Quantum &#39; &amp; coherence\n"
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdin, sys.stdout = io.StringIO(csv_text), io.StringIO()
        try:
            mapper_module.process_stream()
            emitted = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_stdout
        self.assertNotIn("39\t1", emitted)
        self.assertNotIn("amp\t1", emitted)
        self.assertIn("quantum\t1", emitted)
        self.assertIn("coherence\t1", emitted)

    def test_reducer_process_stream_aggregates_and_filters(self):
        """The reducer merges sorted keys and drops sub-threshold terms."""
        stream = "alpha\t2\nalpha\t3\nbeta\t1\ngamma\t5\nbadline\n\n"
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdin, sys.stdout = io.StringIO(stream), io.StringIO()
        try:
            reducer_module.process_stream(min_frequency_threshold=5)
            emitted = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_stdout
        self.assertEqual(emitted.splitlines(), ["alpha\t5", "gamma\t5"])

    def test_reducer_default_threshold_and_malformed_lines(self):
        """Default threshold is 5; malformed rows are skipped, not fatal."""
        stream = "solo\t1\nkeep\t4\nkeep\t2\nmalformed\nkeep\tbad\n"
        old_stdin, old_stdout = sys.stdin, sys.stdout
        sys.stdin, sys.stdout = io.StringIO(stream), io.StringIO()
        try:
            reducer_module.process_stream()
            emitted = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_stdout
        self.assertEqual(emitted.splitlines(), ["keep\t6"])

if __name__ == "__main__":
    unittest.main()
