"""
Notebook Re-execution Helper
============================
Runs the master Jupyter notebook in place so every stored output reflects the
current artifacts (Parquet lakes, analytics JSON, charts, dashboards and tests).

Why a script instead of a raw command
-------------------------------------
The notebook starts Spark and prints Unicode, so on Windows the console must be
UTF-8 and Spark must be pinned to loopback. This wrapper sets those variables
deterministically before delegating to ``jupyter nbconvert``.

Usage
-----
    python scripts/reexecute_notebook.py
    python scripts/reexecute_notebook.py --timeout 1800
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = PROJECT_ROOT / "notebooks" / "News_Article_Analysis_Pipeline.ipynb"


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-execute the project notebook in place.")
    parser.add_argument("--timeout", type=int, default=1200,
                        help="Per-cell execution timeout in seconds (default: 1200).")
    args = parser.parse_args()

    if not NOTEBOOK.exists():
        print(f"[ERROR] Notebook not found: {NOTEBOOK}")
        return 1

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    env.setdefault("PYTHONUNBUFFERED", "1")

    cmd = [
        sys.executable, "-m", "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--inplace",
        f"--ExecutePreprocessor.timeout={args.timeout}",
        str(NOTEBOOK),
    ]

    print("[*] Re-executing notebook (this may take several minutes):")
    print("    " + " ".join(cmd))
    started = time.time()
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env)
    elapsed = time.time() - started
    if result.returncode != 0:
        print(f"[!] Notebook execution FAILED after {elapsed:.1f}s (code {result.returncode}).")
        return result.returncode
    print(f"[+] Notebook re-executed successfully in {elapsed:.1f}s: {NOTEBOOK}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
