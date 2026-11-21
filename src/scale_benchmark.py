"""
Scalability / Volume Benchmark (Single-Host, Memory-Bounded)
============================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Authors: 7-Member Big Data Engineering & Analytics Team
Component: Evidence for "why Big Data and not a spreadsheet" review question

Purpose:
--------
The two shipped corpora are deliberately small proof-of-concept samples
(6,000 synthetic articles ~= 5.7 MB; 10,712 Reuters articles ~= 10.7 MB), while
the ingestion adapter `src/ingest_kaggle_real.py` targets "All the News" at
143k / 2.7M articles. This module closes the evidence gap by actually
generating and processing a LARGER full-text corpus locally, measuring the
generation and cleaning/Parquet stages, and writing a small JSON summary that
can be cited in the report and presentation.

Design constraints (important for reviewers):
---------------------------------------------
1. MEMORY-BOUNDED: `generate_realistic_news_dataset()` from
   `src/generate_sample_data.py` builds a whole DataFrame in RAM. We therefore
   call it once per chunk (--chunk-size rows each) with a derived seed, and
   stream those chunk CSVs one at a time through pandas + PyArrow. Peak
   Python memory is O(chunk_size), not O(total rows).
2. NON-DESTRUCTIVE: all bulky artifacts live under
   `data/scale_benchmark_work/run_<timestamp>/`. The shipped canonical corpus
   `data/raw_news_articles.csv` is never read or written by this module.
3. HONEST PROJECTIONS: timings to 143k / 2.7M articles are explicitly labelled
   LINEAR PROJECTION (NOT A MEASUREMENT). They are arithmetic extrapolation of
   measured rows/sec and assume perfectly linear scaling.
4. SELF-CLEANING: the large CSV/Parquet files are removed at the end; only
   `data/scale_benchmark.json` survives (unless --keep-workdir is supplied).

Usage:
------
    python src/scale_benchmark.py                       # 50,000 articles (default)
    python src/scale_benchmark.py --rows 5000           # fast smoke test
    python src/scale_benchmark.py --keep-workdir        # keep artifacts for debugging

Only already-installed dependencies are used (pandas, numpy via the generator,
PyArrow, and the Python standard library). No new pip installs are required.
"""

import argparse
import datetime as _dt
import importlib.util
import json
import os
import platform
import shutil
import sys
import time

# ---------------------------------------------------------------------------
# Project-relative path resolution (works from repo root AND from any cwd)
# ---------------------------------------------------------------------------
# The test suite uses the same `from src.xxx import ...` convention, so add the
# repository root to sys.path once here. The generator/PySpark adapter imports
# stay lazy (inside functions) to keep `import src.scale_benchmark` cheap for
# the offline unit tests.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# ---------------------------------------------------------------------------
# Configuration constants (all overridable on the command line where noted)
# ---------------------------------------------------------------------------
DEFAULT_ROWS = 50_000              # 10x the shipped synthetic demo corpus
DEFAULT_SEED = 42                  # fixed seed => reproducible benchmark
DEFAULT_CHUNK_SIZE = 5_000         # peak in-memory DataFrame size
DEFAULT_WORK_DIR = "data/scale_benchmark_work"
DEFAULT_OUTPUT_JSON = "data/scale_benchmark.json"

# Reviewer-referenced Kaggle "All the News" corpus sizes.
PROJECTION_TARGET_ROWS = (143_000, 2_700_000)

# We quote MB as 1,048,576 bytes (MiB) and say so explicitly in the JSON.
MB_BYTES = 1024 * 1024

# Canonical Parquet export schema. This mirrors the key attributes of
# `spark_pipeline.define_explicit_schema()` and adds `clean_content` so the
# measured bytes represent a genuine FULL-TEXT lakehouse sink rather than only
# the derived analytics columns exported by the main pipeline.
PARQUET_COLUMNS = (
    "article_id", "title", "author", "date", "year", "month",
    "publication", "category", "sub_topic", "clean_content", "clean_word_count",
)
PARQUET_STRING_COLUMNS = (
    "article_id", "title", "author", "date", "publication",
    "category", "sub_topic", "clean_content",
)

# NOTE: projection is deliberately a distinct module-level constant so docs and
# tests can assert that the "not a measurement" disclaimer is always present.
PROJECTION_LABEL = "LINEAR PROJECTION (NOT A MEASUREMENT)"


# ===========================================================================
# PURE HELPERS (unit tested; no I/O, no corpus generation)
# ===========================================================================
def validate_parameters(rows, chunk_size, seed):
    """
    Validates the three numeric benchmark parameters.

    Rejecting nonsense early prevents a multi-hour accidental run (e.g.
    --rows 0 silently producing an empty corpus, or a negative chunk size
    causing pandas to hang). Returns a normalized dict used by the summary.

    Raises
    ------
    ValueError
        If any argument is not an int, or is outside its allowed range.
    """
    for name, value in (("rows", rows), ("chunk_size", chunk_size), ("seed", seed)):
        # bool is an int subclass in Python; excluding it avoids `--rows True`
        # silently becoming one row.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer, got {type(value).__name__}")
    if rows < 1:
        raise ValueError("rows must be >= 1 (a benchmark needs at least one article)")
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1 (pandas read_csv chunksize)")
    if seed < 0:
        raise ValueError("seed must be >= 0 (used to derive per-chunk seeds)")
    return {"rows": rows, "chunk_size": chunk_size, "seed": seed}


def format_bytes(num_bytes):
    """
    Formats a byte count as a human-readable binary string.

    Examples: 512 -> '512 B', 2048 -> '2.00 KiB',
    5,995,232 -> '5.72 MiB'. Used only for console output; the JSON keeps raw
    integer byte counts so reviewers can recompute throughput themselves.
    """
    if isinstance(num_bytes, bool) or not isinstance(num_bytes, (int, float)):
        raise ValueError("num_bytes must be a number")
    if num_bytes < 0:
        raise ValueError("num_bytes must be >= 0")
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{int(value)} B" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024.0
    raise AssertionError("unreachable")  # pragma: no cover - loop always returns


def project_linear(measured_rows, measured_seconds, target_rows, measured_bytes=None):
    """
    Arithmetic LINEAR projection from one measured rate to a target volume.

    This is intentionally simple: projected_seconds = target_rows / (rows/sec).
    It is NOT a measurement and NOT a capacity plan. It assumes per-row cost
    stays constant as volume grows, which real clusters never guarantee.

    Parameters
    ----------
    measured_rows, measured_seconds : numbers
        The actually measured workload and its wall-clock duration.
    target_rows : int
        Corpus size to extrapolate to (e.g. 143000).
    measured_bytes : number or None
        If supplied, also projects output bytes for the target volume.

    Returns
    -------
    dict with target_rows, projected_seconds/minutes/hours, and optionally
    projected_output_mb. All floats are rounded for display in the JSON.
    """
    for name, value in (("measured_rows", measured_rows),
                        ("measured_seconds", measured_seconds),
                        ("target_rows", target_rows)):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be a number")
        if value <= 0:
            raise ValueError(f"{name} must be > 0")
    rows_per_sec = measured_rows / measured_seconds
    seconds = target_rows / rows_per_sec
    payload = {
        "target_rows": int(target_rows),
        "projected_seconds": round(seconds, 2),
        "projected_minutes": round(seconds / 60.0, 2),
        "projected_hours": round(seconds / 3600.0, 3),
    }
    if measured_bytes is not None:
        projected_bytes = measured_bytes * (float(target_rows) / measured_rows)
        payload["projected_output_mb"] = round(projected_bytes / MB_BYTES, 2)
    return payload


def compute_throughput(rows, rows_kept, seconds, input_bytes, output_bytes):
    """
    Derives rows/sec and MB/sec from measured counts, bytes and durations.

    Separate stage rates are kept alongside the end-to-end rate so reviewers
    can see which stage dominates. `MB` follows the MB_BYTES definition above.
    """
    if seconds <= 0:
        raise ValueError("seconds must be > 0 to compute throughput")
    rows = max(int(rows), 0)
    rows_kept = max(int(rows_kept), 0)
    return {
        "generation_rows_per_sec": round(rows / seconds, 2),
        "clean_parquet_rows_per_sec": round(rows_kept / seconds, 2),
        "end_to_end_rows_per_sec": round(rows / seconds, 2),
        "generation_mb_per_sec": round((input_bytes / MB_BYTES) / seconds, 3),
        "parquet_mb_per_sec": round((output_bytes / MB_BYTES) / seconds, 3),
        "end_to_end_mb_per_sec": round((output_bytes / MB_BYTES) / seconds, 3),
        "mb_definition": "1 MB = 1048576 bytes (MiB)",
    }


def build_summary_skeleton():
    """
    Returns the exact key structure required from data/scale_benchmark.json.

    The JSON schema test imports this function, so the schema can never drift
    silently away from what the benchmark actually writes.
    """
    return {
        "generated_at": None,
        "parameters": {
            "rows": None, "seed": None, "chunk_size": None,
            "work_dir": None, "output": None,
        },
        "timings_seconds": {
            "generation": None, "clean_parquet": None, "measured_total": None,
        },
        "bytes": {
            "csv_input_bytes": None, "parquet_output_bytes": None,
            "total_bytes": None, "csv_input_mb": None, "parquet_output_mb": None,
        },
        "throughput": {
            "generation_rows_per_sec": None, "clean_parquet_rows_per_sec": None,
            "end_to_end_rows_per_sec": None, "generation_mb_per_sec": None,
            "parquet_mb_per_sec": None, "end_to_end_mb_per_sec": None,
            "mb_definition": None,
        },
        "projections": {
            "label": None, "assumes_linear_scaling": None, "is_measurement": None,
            "basis": None, "targets": {},
        },
        "environment": {
            "python_version": None, "os": None, "cpu_count": None,
            "pyspark_version": None, "pandas_version": None, "pyarrow_version": None,
        },
        "caveats": [],
    }


# Derive the validation contract from the single source of truth above.
_SKELETON = build_summary_skeleton()
SUMMARY_TOP_LEVEL_KEYS = tuple(_SKELETON.keys())
SUMMARY_NESTED_KEYS = {
    key: tuple(value.keys())
    for key, value in _SKELETON.items()
    if isinstance(value, dict)
}


def validate_summary_schema(payload):
    """
    Checks that a benchmark summary contains every documented key.

    Returns True on success, raises ValueError listing missing keys otherwise.
    Used both as a final internal assertion before writing JSON and by the
    offline unit tests.
    """
    if not isinstance(payload, dict):
        raise ValueError("summary payload must be a dict")
    missing = []
    for key in SUMMARY_TOP_LEVEL_KEYS:
        if key not in payload:
            missing.append(key)
            continue
        if key in SUMMARY_NESTED_KEYS and isinstance(payload[key], dict):
            for nested_key in SUMMARY_NESTED_KEYS[key]:
                if nested_key not in payload[key]:
                    missing.append(f"{key}.{nested_key}")
    if missing:
        raise ValueError("summary payload missing keys: " + ", ".join(missing))
    return True


def build_projections(measured_rows, measured_seconds, measured_bytes):
    """
    Wraps `project_linear()` for the two reviewer-referenced Kaggle volumes and
    stamps the payload with the mandatory 'not a measurement' disclaimer.
    """
    targets = {}
    for target in PROJECTION_TARGET_ROWS:
        targets[str(target)] = project_linear(
            measured_rows, measured_seconds, target, measured_bytes=measured_bytes
        )
    return {
        "label": PROJECTION_LABEL,
        "assumes_linear_scaling": True,
        "is_measurement": False,
        "basis": (
            f"Extrapolated from {measured_rows:,} measured articles at "
            f"{measured_rows / measured_seconds:.2f} rows/sec (generation + "
            "cleaning/Parquet). Assumes constant per-row cost; ignores I/O "
            "saturation, GC, Spark shuffle/network and schema growth."
        ),
        "targets": targets,
    }


# ===========================================================================
# I/O HELPERS (filesystem only; no corpus generation)
# ===========================================================================
def _resolve_project_path(path):
    """Resolves CLI paths relative to the project root, not the caller's cwd."""
    return path if os.path.isabs(path) else os.path.join(REPO_ROOT, path)


def _assert_safe_paths(work_dir, parquet_dir, output_json):
    """Guards against ever writing onto the shipped canonical corpus."""
    canonical_csv = os.path.abspath(os.path.join(REPO_ROOT, "data", "raw_news_articles.csv"))
    for label, path in (("work_dir", work_dir), ("parquet_dir", parquet_dir),
                        ("output_json", output_json)):
        if os.path.abspath(path) == canonical_csv:
            raise ValueError(f"{label} must never point at {canonical_csv}")
    if os.path.abspath(work_dir) == os.path.abspath(os.path.dirname(canonical_csv)):
        raise ValueError("work_dir must be a subdirectory, not data/ itself")


def directory_bytes(root_path):
    """Sums file sizes under a directory; used for CSV/Parquet output bytes."""
    total = 0
    for dirpath, _dirnames, filenames in os.walk(root_path):
        for name in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                # A file may vanish if another agent cleans up concurrently;
                # ignore it rather than failing the whole benchmark.
                continue
    return total


def count_partitions(parquet_root):
    """
    Counts populated `year=YYYY/month=M` partitions, matching the directory
    convention used by `src/spark_pipeline.py` Stage 7.
    """
    if not os.path.isdir(parquet_root):
        return 0
    count = 0
    for year_dir in os.listdir(parquet_root):
        if not year_dir.startswith("year="):
            continue
        year_path = os.path.join(parquet_root, year_dir)
        if not os.path.isdir(year_path):
            continue
        for month_dir in os.listdir(year_path):
            if not month_dir.startswith("month="):
                continue
            month_path = os.path.join(year_path, month_dir)
            if os.path.isdir(month_path) and any(
                name.endswith(".parquet") for name in os.listdir(month_path)
            ):
                count += 1
    return count


def _optional_version(module_name):
    """Returns an installed module's version, or None if it is unavailable."""
    try:
        if importlib.util.find_spec(module_name) is None:
            return None
        module = __import__(module_name)
        return getattr(module, "__version__", "installed (version unknown)")
    except Exception as exc:  # noqa: BLE001 - environment probing must not fail the run
        return f"unavailable ({type(exc).__name__})"


def build_environment():
    """Captures the host/runtime context needed to interpret the numbers."""
    return {
        "python_version": platform.python_version(),
        "os": platform.platform(),
        "cpu_count": os.cpu_count(),
        "pyspark_version": _optional_version("pyspark"),
        "pandas_version": _optional_version("pandas"),
        "pyarrow_version": _optional_version("pyarrow"),
    }


# ===========================================================================
# STAGE 1 - MEMORY-BOUNDED FULL-TEXT GENERATION
# ===========================================================================
def generate_chunk_csvs(run_dir, rows, chunk_size, seed):
    """
    Calls the shipped generator once per chunk and returns the chunk CSV paths.

    `generate_realistic_news_dataset()` resets random/np.random state on every
    call, so passing the SAME seed to every chunk would duplicate articles.
    We derive `seed + chunk_index` instead: deterministic, varied, and still
    fully reproducible from the single --seed value recorded in the JSON.
    """
    # Lazy import: keeps unit-test imports fast and avoids pandas/numpy cost for
    # callers that only use the pure helpers above.
    from src.generate_sample_data import generate_realistic_news_dataset

    os.makedirs(run_dir, exist_ok=True)
    chunk_paths = []
    generated = 0
    chunk_index = 0
    while generated < rows:
        n = min(chunk_size, rows - generated)
        chunk_path = os.path.join(run_dir, f"generated_chunk_{chunk_index:04d}.csv")
        chunk_seed = seed + chunk_index
        generate_realistic_news_dataset(
            output_csv_path=chunk_path,
            num_records=n,
            seed=chunk_seed,
        )
        chunk_paths.append(chunk_path)
        generated += n
        chunk_index += 1
    return chunk_paths, generated


# ===========================================================================
# STAGE 2 - STREAMING CLEAN + PARTITIONED PARQUET WRITE
# ===========================================================================
def _clean_chunk(raw_chunk, global_offset):
    """
    Applies the same text hygiene as `spark_pipeline.py` Stage 2, in pandas.

    Cleaning rules (kept identical to PySpark for comparability):
      - strip HTML tags, URLs, XML/HTML entities, non-alphabetic characters
      - collapse whitespace, trim, lowercase
      - drop articles with fewer than 10 cleaned words
      - impute missing/blank authors with 'Staff Correspondent'
    Full text is retained in `clean_content` (the main Spark analytics export
    keeps only clean_word_count), because this benchmark measures full-text
    storage volume.
    """
    import pandas as pd

    from src.ingest_kaggle_real import TARGET_COLUMNS

    df = raw_chunk.reset_index(drop=True)
    missing = [column for column in TARGET_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"benchmark chunk missing canonical columns: {missing}")
    df = df[list(TARGET_COLUMNS)].copy()

    content = df["content"].fillna("").astype(str)
    clean_content = (
        content.str.replace(r"<[^>]+>", " ", regex=True)          # HTML tags
        .str.replace(r"http[s]?://\S+", " ", regex=True)          # web URLs
        .str.replace(r"&[a-z0-9#]+;", " ", regex=True)            # HTML entities
        .str.replace(r"[^a-zA-Z\s]", " ", regex=True)             # punctuation/digits
        .str.replace(r"\s+", " ", regex=True)                     # collapse whitespace
        .str.strip()
        .str.lower()
    )
    clean_word_count = clean_content.str.count(r"\S+").astype("int32")
    valid = (
        clean_word_count.ge(10)
        & df["article_id"].notna()
        & df["content"].notna()
    )

    df = df.loc[valid].copy()
    df["clean_content"] = clean_content.loc[valid]
    df["clean_word_count"] = clean_word_count.loc[valid]
    df["author"] = (
        df["author"].fillna("Staff Correspondent").replace("", "Staff Correspondent")
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype("int32")
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype("int32")

    # Each chunk was generated independently, so the generator's ART_xxxxxx ids
    # repeat. Renumber globally to preserve the uniqueness contract of the lakehouse.
    df["article_id"] = [f"SCALE_{global_offset + i:08d}" for i in range(len(df))]
    for column in PARQUET_STRING_COLUMNS:
        df[column] = df[column].fillna("").astype(str)
    return df


def clean_and_write_parquet(chunk_paths, parquet_root, chunk_size):
    """
    Streams every chunk CSV through cleaning and into Parquet partitions.

    One PyArrow ParquetWriter is held open per (year, month) partition, so peak
    memory stays bounded by `chunk_size` rows rather than the whole corpus.
    Partition layout matches Spark: `year=YYYY/month=M/part-0.parquet`.

    Returns (rows_in, rows_kept, partition_count).
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_schema = pa.schema([
        ("article_id", pa.string()),
        ("title", pa.string()),
        ("author", pa.string()),
        ("date", pa.string()),
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("publication", pa.string()),
        ("category", pa.string()),
        ("sub_topic", pa.string()),
        ("clean_content", pa.string()),
        ("clean_word_count", pa.int32()),
    ])
    os.makedirs(parquet_root, exist_ok=True)
    writers = {}
    rows_in = 0
    rows_kept = 0

    try:
        for chunk_path in chunk_paths:
            # pandas' chunked reader keeps only one `chunk_size` slice in RAM.
            for raw_chunk in pd.read_csv(
                chunk_path, chunksize=chunk_size, encoding="utf-8", encoding_errors="replace"
            ):
                offset = rows_in  # unique-id namespace across all chunks
                rows_in += len(raw_chunk)
                cleaned = _clean_chunk(raw_chunk, offset)
                if cleaned.empty:
                    continue
                rows_kept += len(cleaned)

                for (year, month), group in cleaned.groupby(["year", "month"], sort=False):
                    key = (int(year), int(month))
                    if key not in writers:
                        partition_dir = os.path.join(
                            parquet_root, f"year={key[0]}", f"month={key[1]}"
                        )
                        os.makedirs(partition_dir, exist_ok=True)
                        writers[key] = pq.ParquetWriter(
                            os.path.join(partition_dir, "part-0.parquet"),
                            parquet_schema,
                            compression="snappy",  # same codec as the Spark pipeline
                        )
                    table = pa.Table.from_pandas(
                        group[list(PARQUET_COLUMNS)],
                        schema=parquet_schema,
                        preserve_index=False,
                    )
                    writers[key].write_table(table)
    finally:
        # Always close writers so Parquet footers are valid even on early error.
        for writer in writers.values():
            writer.close()

    return rows_in, rows_kept, count_partitions(parquet_root)


# ===========================================================================
# ORCHESTRATION
# ===========================================================================
def run_benchmark(args):
    """Executes both measured stages, writes the JSON summary, cleans up."""
    validated = validate_parameters(args.rows, args.chunk_size, args.seed)
    rows = validated["rows"]
    chunk_size = validated["chunk_size"]
    seed = validated["seed"]

    work_root = _resolve_project_path(args.work_dir)
    output_json = _resolve_project_path(args.output)
    # A timestamped run directory means a previous --keep-workdir run is never
    # overwritten or silently deleted by the next invocation.
    stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(work_root, f"run_{stamp}")
    parquet_root = os.path.join(run_dir, "parquet")
    _assert_safe_paths(run_dir, parquet_root, output_json)
    os.makedirs(run_dir, exist_ok=False)

    print("=" * 78)
    print("  LOCAL SCALABILITY / VOLUME BENCHMARK (single host, memory-bounded)")
    print("=" * 78)
    print(f"[*] Articles:      {rows:,}")
    print(f"[*] Chunk size:    {chunk_size:,} rows (peak in-memory frame)")
    print(f"[*] Seed:          {seed} (per-chunk seed = seed + chunk_index)")
    print(f"[*] Work dir:      {run_dir}")
    print(f"[*] Summary JSON:  {output_json}")
    print("[*] Canonical corpus data/raw_news_articles.csv is NEVER touched.")

    try:
        # ---- STAGE 1: generation -------------------------------------------
        gen_start = time.perf_counter()
        chunk_paths, generated = generate_chunk_csvs(run_dir, rows, chunk_size, seed)
        generation_seconds = max(time.perf_counter() - gen_start, 1e-6)
        csv_bytes = sum(os.path.getsize(path) for path in chunk_paths)
        print(f"[+] Generation:    {generated:,} articles in {generation_seconds:.2f}s "
              f"({format_bytes(csv_bytes)} of full-text CSV)")

        # ---- STAGE 2: clean + partitioned Parquet write --------------------
        clean_start = time.perf_counter()
        rows_in, rows_kept, partition_count = clean_and_write_parquet(
            chunk_paths, parquet_root, chunk_size
        )
        clean_seconds = max(time.perf_counter() - clean_start, 1e-6)
        parquet_bytes = directory_bytes(parquet_root)
        print(f"[+] Clean+Parquet: {rows_kept:,}/{rows_in:,} articles kept in "
              f"{clean_seconds:.2f}s ({format_bytes(parquet_bytes)}, "
              f"{partition_count} year/month partitions)")

        if rows_in != rows:
            raise RuntimeError(f"expected {rows} generated rows, measured {rows_in}")

        # ---- Derived metrics -----------------------------------------------
        measured_seconds = generation_seconds + clean_seconds
        throughput = compute_throughput(
            rows, rows_kept, measured_seconds, csv_bytes, parquet_bytes
        )
        # Overwrite stage-specific rates with stage-specific durations. The
        # compute_throughput() helper uses one duration for simplicity; here we
        # refine it with the real per-stage timings for a fairer comparison.
        throughput["generation_rows_per_sec"] = round(rows / generation_seconds, 2)
        throughput["clean_parquet_rows_per_sec"] = round(rows_kept / clean_seconds, 2)
        throughput["generation_mb_per_sec"] = round(
            (csv_bytes / MB_BYTES) / generation_seconds, 3
        )
        throughput["parquet_mb_per_sec"] = round(
            (parquet_bytes / MB_BYTES) / clean_seconds, 3
        )
        projections = build_projections(rows, measured_seconds, parquet_bytes)

        summary = build_summary_skeleton()
        summary["generated_at"] = _dt.datetime.now().isoformat(timespec="seconds")
        summary["parameters"] = {
            "rows": rows,
            "seed": seed,
            "chunk_size": chunk_size,
            "work_dir": os.path.relpath(run_dir, REPO_ROOT).replace("\\", "/"),
            "output": os.path.relpath(output_json, REPO_ROOT).replace("\\", "/"),
        }
        summary["timings_seconds"] = {
            "generation": round(generation_seconds, 3),
            "clean_parquet": round(clean_seconds, 3),
            "measured_total": round(measured_seconds, 3),
        }
        summary["bytes"] = {
            "csv_input_bytes": csv_bytes,
            "parquet_output_bytes": parquet_bytes,
            "total_bytes": csv_bytes + parquet_bytes,
            "csv_input_mb": round(csv_bytes / MB_BYTES, 2),
            "parquet_output_mb": round(parquet_bytes / MB_BYTES, 2),
        }
        summary["throughput"] = throughput
        summary["projections"] = projections
        summary["environment"] = build_environment()
        summary["caveats"] = [
            "Single-host local benchmark on "
            f"{summary['environment']['cpu_count']} logical CPUs; NOT a cluster "
            "benchmark and NOT a Spark LDA/MapReduce end-to-end timing.",
            "Linear projections are arithmetic extrapolations of measured rows/sec, "
            "not measurements. They assume constant per-row cost and ignore I/O "
            "saturation, GC, Spark shuffle/network, and schema growth.",
            "Corpus is synthetic: the shipped generator is called once per chunk "
            "with seed + chunk_index; its limited vocabulary repeats across chunks "
            "unlike a real Kaggle archive.",
            "Timings cover generation and cleaning/Parquet only; interpreter "
            "startup, generator prints, and JSON serialization are excluded.",
            "1 MB = 1,048,576 bytes (MiB). Input CSV bytes are the uncompressed "
            "generated chunks; Parquet bytes are Snappy-compressed outputs.",
            "article_id values are renumbered SCALE_00000000.. in cleaning because "
            "independently generated chunks reuse the generator's ART_ ids.",
            "Temporary full-text CSV/Parquet artifacts are deleted after the run "
            "unless --keep-workdir is passed; only the JSON summary is retained.",
        ]
        # Internal guard: the file we are about to publish must match the schema
        # the offline tests assert against.
        validate_summary_schema(summary)

        os.makedirs(os.path.dirname(output_json) or ".", exist_ok=True)
        with open(output_json, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
            handle.write("\n")
        print(f"[+] Summary written -> {output_json}")
        print(f"[*] End-to-end throughput: "
              f"{throughput['end_to_end_rows_per_sec']:,.2f} rows/sec "
              f"({throughput['end_to_end_mb_per_sec']:.3f} MB/sec)")
        print(f"[*] {PROJECTION_LABEL}: 143k -> "
              f"{projections['targets']['143000']['projected_minutes']:.2f} min, "
              f"2.7M -> {projections['targets']['2700000']['projected_hours']:.2f} h")
    finally:
        if args.keep_workdir:
            print(f"[*] --keep-workdir set: artifacts retained at {run_dir}")
        else:
            shutil.rmtree(run_dir, ignore_errors=True)
            print("[*] Temporary CSV/Parquet artifacts removed; JSON summary kept.")
            # Remove the empty parent only; never deletes pre-existing user data.
            try:
                os.rmdir(work_root)
            except OSError:
                pass

    return output_json


def main(argv=None):
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Measure single-host generation + cleaning/Parquet throughput on a "
            "larger synthetic full-text corpus (never touches raw_news_articles.csv)."
        )
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS,
                        help=f"total articles to generate (default: {DEFAULT_ROWS:,})")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help=f"base random seed (default: {DEFAULT_SEED})")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"rows per in-memory chunk (default: {DEFAULT_CHUNK_SIZE:,})")
    parser.add_argument("--work-dir", default=DEFAULT_WORK_DIR,
                        help=f"temporary artifact root (default: {DEFAULT_WORK_DIR})")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_JSON,
                        help=f"JSON summary path (default: {DEFAULT_OUTPUT_JSON})")
    parser.add_argument("--keep-workdir", action="store_true",
                        help="keep the large temporary CSV/Parquet files for debugging")
    args = parser.parse_args(argv)

    try:
        run_benchmark(args)
    except ValueError as exc:
        print(f"[!] Invalid benchmark configuration: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
