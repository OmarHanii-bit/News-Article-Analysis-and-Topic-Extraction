"""
Unit Tests: Local Scalability / Volume Benchmark
================================================
Component: Quality Assurance & Engineering Verification
Author: David Kim (QA, Benchmarking & Presentation Lead)

Fast OFFLINE tests for the pure helpers in `src/scale_benchmark.py`. These
tests deliberately never call generate_chunk_csvs()/clean_and_write_parquet():
a 50,000-article corpus is a manual benchmark, not a unit-test fixture.

Coverage: projection math, byte formatting, parameter validation, throughput
math, and the JSON summary schema contract cited by SCALABILITY_AND_KAGGLE.md.
"""

import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.scale_benchmark import (  # noqa: E402  (path setup must come first)
    DEFAULT_CHUNK_SIZE,
    DEFAULT_ROWS,
    DEFAULT_SEED,
    MB_BYTES,
    PROJECTION_LABEL,
    PROJECTION_TARGET_ROWS,
    SUMMARY_NESTED_KEYS,
    SUMMARY_TOP_LEVEL_KEYS,
    build_projections,
    build_summary_skeleton,
    compute_throughput,
    format_bytes,
    project_linear,
    validate_parameters,
    validate_summary_schema,
)


class TestParameterValidation(unittest.TestCase):
    """Bad CLI parameters must fail fast, before any corpus is generated."""

    def test_valid_parameters_are_normalized(self):
        """A well-formed triple must pass through unchanged."""
        params = validate_parameters(5000, 1000, 42)
        self.assertEqual(params, {"rows": 5000, "chunk_size": 1000, "seed": 42})

    def test_non_positive_rows_are_rejected(self):
        """--rows 0 / negative would produce an empty or nonsensical benchmark."""
        with self.assertRaises(ValueError):
            validate_parameters(0, 100, 42)
        with self.assertRaises(ValueError):
            validate_parameters(-10, 100, 42)

    def test_non_positive_chunk_size_is_rejected(self):
        """pandas read_csv chunksize must be >= 1 to avoid a hang."""
        with self.assertRaises(ValueError):
            validate_parameters(5000, 0, 42)

    def test_negative_seed_is_rejected(self):
        """Per-chunk seeds are seed + index; negative seeds are not supported."""
        with self.assertRaises(ValueError):
            validate_parameters(5000, 100, -1)

    def test_non_integer_and_boolean_values_are_rejected(self):
        """Bool is an int subclass in Python; accepting it would be surprising."""
        for bad in (True, 1.5, "5000", None):
            with self.assertRaises(ValueError):
                validate_parameters(bad, 100, 42)

    def test_documented_defaults_are_sane(self):
        """Defaults must represent the larger 50k run the report cites."""
        self.assertEqual(DEFAULT_ROWS, 50_000)
        self.assertEqual(DEFAULT_SEED, 42)
        self.assertGreaterEqual(DEFAULT_CHUNK_SIZE, 1)
        self.assertLessEqual(DEFAULT_CHUNK_SIZE, DEFAULT_ROWS)


class TestByteFormatting(unittest.TestCase):
    """Human-readable sizes used only on stdout; JSON keeps raw byte counts."""

    def test_binary_units_and_rounding(self):
        """format_bytes must use 1024-based units with two decimals."""
        self.assertEqual(format_bytes(512), "512 B")
        self.assertEqual(format_bytes(2048), "2.00 KiB")
        self.assertEqual(format_bytes(5 * MB_BYTES), "5.00 MiB")
        self.assertEqual(format_bytes(3 * MB_BYTES * 1024), "3.00 GiB")

    def test_negative_and_non_numeric_bytes_are_rejected(self):
        """A byte count can never be negative or a string."""
        with self.assertRaises(ValueError):
            format_bytes(-1)
        with self.assertRaises(ValueError):
            format_bytes("1000")


class TestProjectionMath(unittest.TestCase):
    """Projections are arithmetic extrapolations, NEVER measurements."""

    def test_linear_projection_uses_measured_rate(self):
        """1000 rows / 2 s = 500 rows/s => 10,000 rows in 20 s."""
        projected = project_linear(1000, 2.0, 10_000)
        self.assertEqual(projected["target_rows"], 10_000)
        self.assertEqual(projected["projected_seconds"], 20.0)
        self.assertEqual(projected["projected_minutes"], round(20 / 60, 2))

    def test_projected_output_bytes_scale_linearly(self):
        """Doubling rows must double projected output bytes (and MB)."""
        base = project_linear(1000, 1.0, 2000, measured_bytes=MB_BYTES)
        self.assertEqual(base["projected_output_mb"], 2.0)

    def test_zero_or_negative_inputs_are_rejected(self):
        """Division by zero would otherwise emit an 'infinite' projection."""
        for args in ((0, 1.0, 1000), (1000, 0.0, 1000), (1000, 1.0, -5)):
            with self.assertRaises(ValueError):
                project_linear(*args)

    def test_projection_payload_carries_the_not_a_measurement_disclaimer(self):
        """Reviewers must never mistake an extrapolation for a measurement."""
        payload = build_projections(1000, 2.0, MB_BYTES)
        self.assertEqual(payload["label"], PROJECTION_LABEL)
        self.assertIn("NOT A MEASUREMENT", payload["label"])
        self.assertTrue(payload["assumes_linear_scaling"])
        self.assertFalse(payload["is_measurement"])
        self.assertEqual(set(payload["targets"]), {str(t) for t in PROJECTION_TARGET_ROWS})
        self.assertEqual(payload["targets"]["143000"]["projected_seconds"], 286.0)


class TestThroughputMath(unittest.TestCase):
    """Rows/sec and MB/sec must be recomputable from the JSON byte fields."""

    def test_stage_rates_and_mb_definition(self):
        """200 rows in 2 s and 1 MiB output => 100 rows/s, 0.5 MB/s."""
        rates = compute_throughput(200, 180, 2.0, 2 * MB_BYTES, MB_BYTES)
        self.assertEqual(rates["end_to_end_rows_per_sec"], 100.0)
        self.assertEqual(rates["clean_parquet_rows_per_sec"], 90.0)
        self.assertEqual(rates["end_to_end_mb_per_sec"], 0.5)
        self.assertIn("1048576", rates["mb_definition"])

    def test_zero_duration_is_rejected(self):
        """A zero duration must not silently produce infinity in the JSON."""
        with self.assertRaises(ValueError):
            compute_throughput(100, 100, 0.0, 0, 0)


class TestSummaryJsonSchema(unittest.TestCase):
    """The published JSON contract must stay in sync with the implementation."""

    def test_skeleton_contains_all_documented_keys(self):
        """Top-level and nested keys must be present exactly once."""
        payload = build_summary_skeleton()
        self.assertEqual(tuple(payload.keys()), SUMMARY_TOP_LEVEL_KEYS)
        for key, nested in SUMMARY_NESTED_KEYS.items():
            self.assertEqual(tuple(payload[key].keys()), nested)

    def test_complete_payload_passes_validation(self):
        """The skeleton produced by the module must satisfy its own validator."""
        self.assertTrue(validate_summary_schema(build_summary_skeleton()))

    def test_missing_top_level_and_nested_keys_are_detected(self):
        """Dropping either a top-level or nested key must be reported."""
        payload = build_summary_skeleton()
        del payload["environment"]
        with self.assertRaises(ValueError) as top_error:
            validate_summary_schema(payload)
        self.assertIn("environment", str(top_error.exception))

        payload = build_summary_skeleton()
        del payload["timings_seconds"]["clean_parquet"]
        with self.assertRaises(ValueError) as nested_error:
            validate_summary_schema(payload)
        self.assertIn("timings_seconds.clean_parquet", str(nested_error.exception))

    def test_schema_survives_a_json_round_trip(self):
        """data/scale_benchmark.json must remain schema-valid after serialization."""
        payload = build_summary_skeleton()
        payload["projections"] = build_projections(1000, 2.0, MB_BYTES)
        restored = json.loads(json.dumps(payload))
        self.assertTrue(validate_summary_schema(restored))
        self.assertFalse(restored["projections"]["is_measurement"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
