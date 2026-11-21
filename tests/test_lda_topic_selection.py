"""
Unit Tests: LDA Topic-Selection Diagnostics (pure math, no Spark session)
=========================================================================
Component: Model Selection / Statistical Verification
Author: Elena Rostova (AI, ML & NLP Lead)

These tests exercise only the deterministic, driver-side helpers in
src/lda_topic_selection.py. They intentionally do NOT create a SparkSession
or require NLTK data, so they run quickly and fully offline.
"""

import math
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.lda_topic_selection import (  # noqa: E402 - path set above
    HELD_OUT_SEED,
    TRAIN_FRACTION,
    heldout_split_indices,
    heldout_split_weights,
    select_k,
    topic_overlap,
    distinctness,
    umass_coherence,
    validate_metrics,
)


class TestCoherenceMath(unittest.TestCase):
    """UMass-style coherence is negative and approaches 0 for tight topics."""

    def test_identical_term_sets_give_zero_coherence(self):
        tokens = [{"alpha", "beta"}] * 3
        self.assertAlmostEqual(umass_coherence(["alpha", "beta"], tokens), 0.0)

    def test_disjoint_terms_give_negative_coherence(self):
        tokens = [{"alpha"}, {"beta"}]
        score = umass_coherence(["alpha", "beta"], tokens)
        self.assertLess(score, 0.0)
        self.assertAlmostEqual(score, math.log(0.5))

    def test_no_documents_returns_nan_not_zero(self):
        self.assertTrue(math.isnan(umass_coherence(["alpha", "beta"], [])))


class TestTopicOverlapMath(unittest.TestCase):
    """Internally renamed metric; JSON key 'distinctness' stays compatible."""

    def test_jaccard_overlap_value(self):
        self.assertAlmostEqual(topic_overlap([{"a", "b", "c"}, {"a", "b"}]), 2.0 / 3.0)

    def test_disjoint_topics_have_zero_overlap(self):
        self.assertEqual(topic_overlap([{"a"}, {"b"}, {"c"}]), 0.0)

    def test_single_topic_is_na(self):
        self.assertIsNone(topic_overlap([{"a", "b"}]))
        self.assertIs(distinctness, topic_overlap)  # backward-compatible alias


class TestHeldOutSplit(unittest.TestCase):
    """Fixed-seed split is reproducible without touching Spark."""

    def test_split_is_deterministic_and_partitions_all_rows(self):
        train_a, test_a = heldout_split_indices(100)
        train_b, test_b = heldout_split_indices(100)
        self.assertEqual(train_a, train_b)
        self.assertEqual(test_a, test_b)
        self.assertEqual(len(train_a), 80)
        self.assertEqual(len(test_a), 20)
        self.assertEqual(sorted(train_a + test_a), list(range(100)))

    def test_different_seed_changes_the_split(self):
        self.assertNotEqual(heldout_split_indices(100, seed=HELD_OUT_SEED)[0],
                            heldout_split_indices(100, seed=HELD_OUT_SEED + 1)[0])

    def test_weights_and_small_corpus_guard(self):
        weights = heldout_split_weights()
        self.assertEqual(weights, [TRAIN_FRACTION, 1.0 - TRAIN_FRACTION])
        self.assertAlmostEqual(sum(weights), 1.0)
        with self.assertRaises(ValueError):
            heldout_split_indices(1)


class TestDegenerateMetricValidation(unittest.TestCase):
    """Degenerate diagnostics must be flagged, never silently reported as 0.0."""

    def test_empty_coherence_is_nan_and_flagged(self):
        topics = [[letter] for letter in "abcdef"]
        mean_coherence, overlap, flags = validate_metrics(6, [], topics)
        self.assertTrue(math.isnan(mean_coherence))
        self.assertTrue(flags.get("coherence_empty"))
        self.assertEqual(overlap, 0.0)  # distinct single-term topics

    def test_overlap_is_na_for_single_topic(self):
        mean_coherence, overlap, flags = validate_metrics(1, [-0.25], [["a", "b"]])
        self.assertAlmostEqual(mean_coherence, -0.25)
        self.assertIsNone(overlap)
        self.assertTrue(flags.get("topic_overlap_na"))

    def test_nonfinite_coherence_scores_are_flagged(self):
        _, _, flags = validate_metrics(3, [-0.1, float("inf"), -0.2],
                                       [["a"], ["b"], ["c"]])
        self.assertTrue(flags.get("coherence_nonfinite"))


class TestSelectionRule(unittest.TestCase):
    """The pre-registered rule must degrade safely on degenerate inputs."""

    def test_all_nan_coherence_raises_instead_of_selecting(self):
        records = [{"k": k, "mean_coherence": None, "log_perplexity": 5.0,
                    "distinctness": None} for k in range(3, 6)]
        with self.assertRaises(ValueError):
            select_k(records)

    def test_na_overlap_does_not_block_knee_selection(self):
        records = [
            {"k": 1, "mean_coherence": -0.50, "log_perplexity": 6.00, "distinctness": None},
            {"k": 2, "mean_coherence": -0.30, "log_perplexity": 5.50, "distinctness": 0.01},
            {"k": 3, "mean_coherence": -0.29, "log_perplexity": 5.20, "distinctness": 0.02},
            {"k": 4, "mean_coherence": -0.285, "log_perplexity": 5.10, "distinctness": 0.03},
        ]
        selected, rationale, monotone = select_k(records)
        # k=3 is the coherence knee, but overlap worsens -> fall back to k=2.
        self.assertEqual(selected, 2)
        self.assertTrue(monotone)
        self.assertIn("topic overlap", rationale.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
