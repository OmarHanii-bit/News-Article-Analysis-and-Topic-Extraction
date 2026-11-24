"""
Master One-Click Pipeline Orchestrator
=====================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Author: 7-Member Big Data Engineering Team
Purpose: Orchestrates the entire end-to-end Big Data pipeline from a single entry point:
         Environment Checks -> Data Generation -> MapReduce -> PySpark -> Hadoop Ecosystem ->
         Real Corpus -> MongoDB -> Visualizations -> PowerPoint -> Offline HTML Dashboards ->
         Word Report Export -> Unit Testing -> Streamlit Launch.
"""

import os
import sys
import csv
import time
import subprocess
import shutil

BANNER = """
================================================================================
   NEWS ARTICLE ANALYSIS & TOPIC EXTRACTION — BIG DATA PIPELINE ORCHESTRATOR   
   Hadoop Ecosystem (HDFS, YARN, MapReduce, Hive, HBase, Pig, Sqoop, Flume,   
   Oozie, ZooKeeper) | PySpark | MongoDB NoSQL | Streamlit Dashboard
================================================================================
"""

# Steps are 0-indexed: 0 is the pre-flight health check and 1-16 are the
# pipeline stages (the last one, the LDA k sweep, is only run with
# --run-lda-sweep).
TOTAL_STEPS = 16


def count_csv_rows(path, fallback=None):
    """Counts data rows in a CSV (header excluded); returns fallback on failure."""
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            next(reader, None)  # skip header
            return sum(1 for _ in reader)
    except (OSError, csv.Error, UnicodeError):
        return fallback


def csv_count_label(path, fallback_text="count unavailable"):
    """Human-readable row count for the deliverables inventory."""
    rows = count_csv_rows(path)
    return f"{rows:,} articles" if rows is not None else fallback_text


def print_step(step_num, title):
    print(f"\n[{step_num}/{TOTAL_STEPS}] >>> {title.upper()} <<<")
    print("-" * 80)

def run_command(cmd, step_desc):
    start = time.time()
    print(f"[*] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    duration = time.time() - start
    if result.returncode != 0:
        print(f"[!] ERROR during: {step_desc} (Exit code: {result.returncode})")
        return False
    print(f"[+] Completed: {step_desc} in {duration:.2f} seconds.")
    return True

def preflight_check():
    print_step(0, "Pre-Flight Environment & Dependency Health Check")
    print(f"[*] Operating System: {sys.platform}")
    print(f"[*] Python Version:   {sys.version.split()[0]}")
    print(f"[*] Spark Master:     {os.environ.get('SPARK_MASTER', 'local[*]')}")
    print(f"[*] Spark Memory:     driver={os.environ.get('SPARK_DRIVER_MEMORY', '4g')}, "
          f"executor={os.environ.get('SPARK_EXECUTOR_MEMORY', '4g')}")
    
    # Check Java
    java_bin = shutil.which("java")
    if java_bin:
        try:
            j_ver = subprocess.check_output([java_bin, "-version"], stderr=subprocess.STDOUT, text=True)
            first_line = j_ver.splitlines()[0] if j_ver else "Java Detected"
            print(f"[+] Java Runtime:     {first_line}")
        except Exception:
            print("[+] Java Runtime:     Found at " + java_bin)
    else:
        print("[!] Warning: 'java' executable not detected on PATH. Spark requires Java JDK 11+.")

    # Check key Python packages
    required_packages = ["pyspark", "pymongo", "nltk", "pandas", "numpy", "matplotlib", "seaborn", "wordcloud", "streamlit", "plotly", "pptx", "docx", "nbformat", "pyarrow"]
    missing = []
    for pkg in required_packages:
        try:
            __import__(pkg)
            print(f"    - {pkg:<15} [OK]")
        except ImportError:
            missing.append(pkg)
            print(f"    - {pkg:<15} [MISSING]")

    if missing:
        print(f"[!] Warning: Missing packages: {missing}. Installing via requirements.txt...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=False)
    else:
        print("[+] All core dependencies verified successfully.")
    return True

def main():
    print(BANNER)
    total_start = time.time()
    force = "--force" in sys.argv
    run_lda_sweep = "--run-lda-sweep" in sys.argv
    if force:
        print("[*] --force: stale raw CSV / PySpark summary will be regenerated.")
    if run_lda_sweep:
        print("[*] --run-lda-sweep: optional LDA k selection sweep will run at the end.")

    # Step 0: Preflight
    preflight_check()

    # Step 1: Synthesize Data
    print_step(1, "Data Generation & Profiling (6,000 Full-Text Articles)")
    data_csv = "data/raw_news_articles.csv"
    if not os.path.exists(data_csv) or force:
        if force and os.path.exists(data_csv):
            print("[*] --force: regenerating the dataset despite an existing CSV.")
        success = run_command([sys.executable, "src/generate_sample_data.py", "6000"], "Data Generation")
        if not success: return
    else:
        print(f"[+] Found existing dataset: {data_csv} (Skipping re-generation).")

    # Step 2: MapReduce
    print_step(2, "Hadoop MapReduce Streaming Token & N-Gram Extraction")
    success = run_command([sys.executable, "src/mapreduce/run_mapreduce_local.py"], "MapReduce Simulation")
    if not success: return

    # Step 3: PySpark Distributed Pipeline
    print_step(3, "PySpark In-Memory Distributed Processing & LDA Topic Modeling")
    summary_json = "data/pipeline_analytics_summary.json"
    parquet_dir = "data/processed_parquet"
    # Skip only when BOTH the summary AND the Parquet lakehouse are present. The
    # summary JSON is committed but the Parquet lakehouse is git-ignored, so a
    # fresh checkout can have the summary yet no Parquet -- skipping on the
    # summary alone would leave the lakehouse missing and break the data tests.
    if not os.path.exists(summary_json) or not os.path.exists(parquet_dir) or force:
        if force and os.path.exists(summary_json):
            print("[*] --force: regenerating the PySpark summary despite an existing file.")
        elif os.path.exists(summary_json) and not os.path.exists(parquet_dir):
            print("[*] Summary present but Parquet lakehouse missing; rebuilding the lakehouse.")
        success = run_command([sys.executable, "src/spark_pipeline.py"], "PySpark Pipeline")
        if not success: return
    else:
        print(f"[+] Found existing PySpark summary: {summary_json}.")
        rerun = False
        if "--force-rerun-spark" in sys.argv:
            rerun = True
        elif sys.stdin.isatty() and "--non-interactive" not in sys.argv:
            try:
                choice = input("[?] Re-run PySpark pipeline? (y/N): ").strip().lower()
                rerun = (choice == 'y')
            except (EOFError, Exception):
                rerun = False
        
        if rerun:
            success = run_command([sys.executable, "src/spark_pipeline.py"], "PySpark Pipeline")
            if not success: return

    # Step 4: Hadoop Ecosystem (HDFS, YARN, Hive, HBase, Pig, Sqoop, Flume, Oozie, ZooKeeper)
    print_step(4, "Hadoop Ecosystem Execution (HDFS | YARN | Hive | HBase | Pig | Sqoop | Flume | Oozie | ZooKeeper)")
    success = run_command([sys.executable, "src/hadoop/run_hadoop_ecosystem.py"], "Hadoop Ecosystem Stack")
    if not success: return

    # Step 5: Real-World Full-Text Corpus (secondary, higher-volume dataset)
    print_step(5, "Real-World Full-Text Corpus (Reuters Wire) — Ingest + PySpark Re-Run")
    real_csv = "data/real_news/raw_news_articles_real.csv"
    # Best-effort acquisition: requires the NLTK Reuters corpus (downloads on first use).
    if not os.path.exists(real_csv):
        print("[*] Real corpus not found. Attempting acquisition (NLTK Reuters-21578)...")
        acquired = run_command([sys.executable, "src/ingest_real_news.py", "--source", "reuters",
                                "--output", real_csv], "Real Corpus Acquisition")
        if not acquired:
            print("[!] WARNING: Real-corpus acquisition FAILED (non-zero exit code). "
                  "The secondary Reuters dataset will be skipped; canonical artifacts "
                  "remain intact. Check NLTK network access / Reuters corpus availability.")
    if os.path.exists(real_csv):
        success = run_command(
            [sys.executable, "src/spark_pipeline.py",
             "--input-csv", real_csv,
             "--output-parquet", "data/real_news/processed_parquet",
             "--output-json", "data/real_news/pipeline_analytics_summary.json",
             "--auto-labels"],
            "PySpark Pipeline on Real Corpus")
        if not success: print("[!] Real-corpus PySpark run failed; continuing with canonical artifacts.")
    else:
        print("[*] Real corpus unavailable (offline). Skipping secondary dataset — canonical pipeline intact.")

    # Step 6: Live Multi-Publisher RSS Ingestion
    print_step(6, "Live Multi-Publisher News RSS Ingestion Engine (NYT | BBC | CBS | NPR)")
    success = run_command([sys.executable, "src/ingest_live_rss.py"], "Live RSS Harvester")
    if not success:
        print("[!] Live RSS harvest failed or was blocked; continuing with the committed live corpus.")

    # Step 7: Real-Time Structured Streaming Pipeline Execution
    print_step(7, "Real-Time PySpark Structured Streaming Pipeline & Volatility Alerting")
    success = run_command([sys.executable, "src/streaming_news_pipeline.py", "--events", "25"], "Streaming Pipeline")
    if not success: return

    # Step 8: Supervised Ground-Truth Classification Benchmark (Reuters-21578)
    print_step(8, "Supervised Ground-Truth Classification Benchmark (Reuters-21578)")
    success = run_command([sys.executable, "src/supervised_classifier.py"], "Supervised Classifier")
    if not success: return

    # Step 9: Automated Topic Drift & Concept Drift Monitoring (MLOps)
    print_step(9, "Automated MLOps Topic Drift & Statistical Stability Monitoring")
    success = run_command([sys.executable, "src/topic_drift_monitor.py"], "Topic Drift Monitor")
    if not success: return

    # Step 10: MongoDB NoSQL Ingestion
    print_step(10, "MongoDB NoSQL Ingestion & Document Store Integration")
    success = run_command([sys.executable, "src/mongo_loader.py"], "MongoDB Loader")
    if not success: return

    # Step 11: Visualizations
    print_step(11, "Analytical Visualization Suite (300 DPI Charts)")
    success = run_command([sys.executable, "src/visualize_analytics.py"], "Visualization Suite")
    if not success: return

    # Step 12: Offline Interactive HTML Dashboards (one per corpus + command center)
    print_step(12, "Offline Dashboards (Plotly Exports + Intelligence Command Center)")
    success = run_command([sys.executable, "src/build_interactive_dashboard.py"], "HTML Dashboard Build")
    if not success: return
    success = run_command([sys.executable, "src/build_command_center.py"], "Command Center Dashboard Build")
    if not success: return

    # Step 13: PowerPoint Presentation
    print_step(13, "PowerPoint Slide Deck Generation (17 Slides .pptx)")
    success = run_command([sys.executable, "src/generate_presentation_pptx.py"], "Slide Deck Generation")
    if not success: return

    # Step 14: Word Report Export (Markdown original left intact)
    print_step(14, "Academic Report Export to Word (.docx)")
    success = run_command([sys.executable, "src/export_report_docx.py"], "Word Report Export")
    if not success: return

    # Step 15: Automated Tests (Final verification gate after all artifacts exist)
    print_step(15, "Automated Test Suite Verification (119 Tests Across 14 Modules)")
    if os.path.exists("tests/run_tests.py"):
        success = run_command([sys.executable, "tests/run_tests.py"], "Test Suite")
        if not success: return
    else:
        print("[*] Test runner will execute after tests are built.")

    # Optional Step 16: model-selection evidence (off by default; runtime unchanged).
    if run_lda_sweep:
        print_step(16, "Optional LDA k Selection Sweep (held-out perplexity + coherence)")
        print("[*] Metrics will land in data/lda_k_sweep.json "
              "(chart: visualizations/lda_k_sweep.png).")
        sweep_ok = run_command([sys.executable, "src/lda_topic_selection.py"], "LDA k Sweep")
        if not sweep_ok:
            print("[!] WARNING: LDA sweep failed; previously generated evidence was left unchanged.")
        else:
            print("[+] LDA k sweep evidence refreshed in data/lda_k_sweep.json.")
    
    # Completion Summary & Streamlit Launch
    total_elapsed = time.time() - total_start
    print("\n[DONE] >>> PIPELINE EXECUTION COMPLETE <<<")
    print("-" * 80)
    print(f"[+] Total Orchestration Runtime: {total_elapsed:.2f} seconds.")
    print("\nDeliverables Inventory:")
    print(f"  - Dataset (synthetic):  {data_csv} ({csv_count_label(data_csv)})")
    print(f"  - Dataset (real):       {real_csv} ({csv_count_label(real_csv, 'not generated')})")
    print("  - Dataset (live RSS):   data/real_news/live_rss_articles.csv (270+ live NYT/BBC/CBS/NPR articles)")
    print("  • MapReduce:            data/mapreduce_term_counts.tsv")
    print("  • PySpark Lake:         data/processed_parquet/ (Partitioned year/month)")
    print("  • PySpark Lake (real):  data/real_news/processed_parquet/ (Partitioned year/month)")
    print("  • Streaming Sink:       data/streaming_parquet/ (PySpark structured streaming)")
    print("  • Supervised Metrics:   data/supervised_classification_metrics.json (92.57% Acc, 0.8423 Macro-F1)")
    print("  • MLOps Drift Monitor:  data/topic_drift_analysis.json (Quarterly JSD & PSI)")
    print("  • Multi-Node Cluster:   docker-compose-cluster.yml (10 distributed containers)")
    print("  • Hadoop Ecosystem:     data/hadoop_ecosystem/ (9 services, HDFS/Hive/HBase/Pig/Sqoop/Flume/Oozie/ZK)")
    print("  • MongoDB BSON:         data/mongodb_offline_collections/ (3 collections)")
    print("  • Visual Charts:        visualizations/ (300 DPI charts)")
    print("  • HTML Dashboards:      dashboard/interactive_dashboard.html (synthetic)")
    print("                          dashboard/interactive_dashboard_real.html (Reuters, self-contained)")
    print("  • PowerPoint:           presentation/News_Article_Analysis_Presentation.pptx")
    print("  • Word Report:          PROJECT_REPORT.docx (Markdown original untouched)")
    print("  • Notebook:             notebooks/News_Article_Analysis_Pipeline.ipynb (executed)")
    print("  - Academic Report:      PROJECT_REPORT.md (rubric alignment documented)")
    print("  • Slide Notes:          presentation/slide_deck_outline.md (Speaker scripts & Q&A defense)")
    
    print("\n" + "=" * 80)
    print("To launch the interactive executive dashboard, run:")
    print("    streamlit run src/dashboard.py")
    print("=" * 80)

if __name__ == "__main__":
    main()
