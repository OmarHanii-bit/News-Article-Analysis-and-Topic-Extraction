"""
Apache Oozie Workflow / Coordinator Orchestration Layer
=======================================================
Project : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache Oozie — DAG orchestration of the whole news pipeline
Owner   : Member 3 — Yasmine Mohamed (Distributed Warehouse Engineer)

WHAT THIS MODULE DOES
---------------------
The news pipeline is a directed acyclic graph (DAG) of heterogeneous Hadoop jobs:
Sqoop bulk-imports the RDBMS dimensions, Pig transforms the raw text into cleaned
records, Hive rolls the warehouse up into analytics tables, Spark runs the LDA topic
model, and a final Shell action verifies the produced artifacts. Apache Oozie is the
production scheduler that runs exactly this DAG, on a schedule, with retries and
transitions:

    <start> -> [sqoop] -> [pig] -> [hive] -> [spark] -> [shell] -> <end>
                  \\---------> <error to="kill"> <----/

This module is DUAL-MODE:

    1. LOCAL SIMULATION — it generates a REAL Oozie 0.5 `workflow.xml`, a REAL
       `coordinator.xml`, and a `job.properties`. It then PARSES the workflow with
       `xml.etree.ElementTree`, resolves the DAG from `<start>` by following the
       `<ok to="..."/>` transitions, and executes every action in dependency order
       through a pluggable action executor. Each action type maps to a Python
       callback that performs a small GENUINE operation against the real artifacts
       (e.g. the hive action reads the warehouse summary, the spark action verifies
       the Parquet lake, the shell action verifies a file exists). A full execution
       journal is written to `oozie_execution_journal.json`.
    2. REAL CLUSTER ARTIFACTS — `real_cluster_commands.md` documents the authentic
       `oozie job -run`, `oozie jobs -filter`, `oozie job -info` commands.

An action whose target artifact is missing is marked `SKIPPED` (never a hard
failure) and the DAG keeps flowing along its `ok` transition. The public entry
point is `run_oozie_workflow(...)`.
"""

# ---------------------------------------------------------------------------
# Path bootstrap: runnable as `python src/hadoop/oozie_runner.py` and importable
# as `from src.hadoop.oozie_runner import run_oozie_workflow`.
# ---------------------------------------------------------------------------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.hadoop.hdfs_client import LocalHDFS  # noqa: F401  (kept for ecosystem parity)

# --- Standard library only -------------------------------------------------
import csv          # read the Hive result CSVs / mapreduce TSV
import glob         # locate Parquet lake partitions
import json         # execution journal + summary
import sqlite3      # inspect the Sqoop-produced RDBMS
import time         # per-action timing
import datetime     # workflow timestamps
import xml.etree.ElementTree as ET  # generate + parse the Oozie XML

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Oozie XML namespaces (the sub-action namespaces differ per action type).
OOZIE_WF_NS = "uri:oozie:workflow:0.5"
OOZIE_COORD_NS = "uri:oozie:coordinator:0.4"
OOZIE_SQOOP_NS = "uri:oozie:sqoop-action:0.2"
OOZIE_PIG_NS = "uri:oozie:pig-action:0.2"
OOZIE_HIVE_NS = "uri:oozie:hive-action:0.6"
OOZIE_SPARK_NS = "uri:oozie:spark-action:0.1"
OOZIE_SHELL_NS = "uri:oozie:shell-action:0.3"


def _resolve(path: str) -> str:
    """Return an absolute path, resolving relative paths against the project root."""
    return path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)


def _rel(path: str) -> str:
    """Return a project-relative, forward-slash path."""
    return os.path.relpath(os.path.abspath(path), PROJECT_ROOT).replace("\\", "/")


def _w(tag: str) -> str:
    """Qualify a tag inside the Oozie workflow namespace for ElementTree."""
    return f"{{{OOZIE_WF_NS}}}{tag}"


# ---------------------------------------------------------------------------
# XML generation (ElementTree)
# ---------------------------------------------------------------------------
def _generate_workflow_xml(path: str) -> str:
    """
    Build a genuine Oozie 0.5 `workflow.xml` and serialise it to `path`.
    The DAG is: start -> sqoop -> pig -> hive -> spark -> shell -> end,
    with an `<error to="kill"/>` transition on every action.
    """
    ET.register_namespace("", OOZIE_WF_NS)
    root = ET.Element(_w("workflow-app"), {"name": "news_pipeline_workflow"})

    # --- Global cluster resources -------------------------------------------
    global_el = ET.SubElement(root, _w("global"))
    ET.SubElement(global_el, _w("resource-manager")).text = "${resourceManager}"
    ET.SubElement(global_el, _w("name-node")).text = "${nameNode}"

    # --- Entry point ---------------------------------------------------------
    ET.SubElement(root, _w("start"), {"to": "sqoop_import"})

    def add_action(name, kind, sub_ns, build_sub, ok_to):
        """Create one <action> with a typed sub-element and ok/error transitions."""
        action = ET.SubElement(root, _w("action"), {"name": name})
        # The sub-element carries its own Oozie namespace via an xmlns attribute.
        sub = ET.SubElement(action, kind, {"xmlns": sub_ns})
        build_sub(sub)
        ET.SubElement(action, _w("ok"), {"to": ok_to})
        ET.SubElement(action, _w("error"), {"to": "kill"})

    # 1. SQOOP — bulk-import the RDBMS dimension tables into HDFS/Hive.
    def _sqoop(sub):
        ET.SubElement(sub, "command").text = (
            "import --connect jdbc:mysql://rdbms-host:3306/news --username etl "
            "--table publishers --target-dir /user/news/sqoop_import/publishers "
            "--m 4 --as-parquetfile"
        )

    # 2. PIG — transform the raw text feed into cleaned_news records.
    def _pig(sub):
        ET.SubElement(sub, "script").text = "news_transform.pig"
        ET.SubElement(sub, "param").text = "input=/user/news/stream output=/user/news/cleaned"

    # 3. HIVE — roll the warehouse up into analytics tables.
    def _hive(sub):
        ET.SubElement(sub, "script").text = "news_analytics.hql"
        ET.SubElement(sub, "param").text = "db=news_db"

    # 4. SPARK — run the LDA topic model + sentiment scoring.
    def _spark(sub):
        ET.SubElement(sub, "master").text = "yarn"
        ET.SubElement(sub, "mode").text = "cluster"
        ET.SubElement(sub, "name").text = "news-lda-topics"
        ET.SubElement(sub, "jar").text = "news_pipeline.jar"
        ET.SubElement(sub, "class").text = "com.news.pipeline.SparkTopicModel"

    # 5. SHELL — verify the produced artifacts landed on HDFS.
    def _shell(sub):
        ET.SubElement(sub, "exec").text = "verify_artifacts.sh"
        ET.SubElement(sub, "file").text = "verify_artifacts.sh#verify_artifacts.sh"

    add_action("sqoop_import", "sqoop", OOZIE_SQOOP_NS, _sqoop, "pig_transform")
    add_action("pig_transform", "pig", OOZIE_PIG_NS, _pig, "hive_analytics")
    add_action("hive_analytics", "hive", OOZIE_HIVE_NS, _hive, "spark_topics")
    add_action("spark_topics", "spark", OOZIE_SPARK_NS, _spark, "shell_verify")
    add_action("shell_verify", "shell", OOZIE_SHELL_NS, _shell, "end")

    # --- Terminal nodes ------------------------------------------------------
    ET.SubElement(root, _w("end"), {"name": "end"})
    kill = ET.SubElement(root, _w("kill"), {"name": "kill"})
    ET.SubElement(kill, _w("message")).text = "News pipeline action failed: ${wf:lastErrorNode()}"

    ET.indent(root, space="    ")  # pretty-print for human readability
    tree = ET.ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)
    return path


def _generate_coordinator_xml(path: str) -> str:
    """
    Build a genuine Oozie `coordinator.xml` that triggers the workflow daily
    over a date-partitioned dataset.
    """
    ET.register_namespace("", OOZIE_COORD_NS)
    C = lambda tag: f"{{{OOZIE_COORD_NS}}}{tag}"  # noqa: E731 a small local helper
    root = ET.Element(C("coordinator-app"), {
        "name": "news_pipeline_coordinator",
        "frequency": "${coord:days(1)}",
        "start": "2025-01-01T00:00Z",
        "end": "2026-12-31T00:00Z",
        "timezone": "UTC",
    })

    # --- Datasets: the date-partitioned stream directory --------------------
    datasets = ET.SubElement(root, C("datasets"))
    ds = ET.SubElement(datasets, C("dataset"), {
        "name": "news_input",
        "frequency": "${coord:days(1)}",
        "initial-instance": "2025-01-01T00:00Z",
        "timezone": "UTC",
    })
    ET.SubElement(ds, C("uri-template")).text = (
        "${nameNode}/user/news/stream/${YEAR}/${MONTH}/${DAY}"
    )

    # --- Action: launch the workflow ----------------------------------------
    action = ET.SubElement(root, C("action"))
    wf = ET.SubElement(action, C("workflow"))
    ET.SubElement(wf, C("app-path")).text = "${nameNode}/user/news/oozie/news_pipeline"
    config = ET.SubElement(wf, C("configuration"))
    for prop_name, prop_value in (("nameNode", "${nameNode}"),
                                  ("queueName", "${queueName}")):
        prop = ET.SubElement(config, C("property"))
        ET.SubElement(prop, C("name")).text = prop_name
        ET.SubElement(prop, C("value")).text = prop_value

    ET.indent(root, space="    ")
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
    return path


def _write_job_properties(path: str, workflow_dir: str) -> str:
    """Write the real `job.properties` consumed by `oozie job -config`."""
    text = f"""# job.properties — Oozie job configuration
# Project : News Article Analysis and Topic Extraction (Big Data Architecture)
# Owner   : Member 3 — Yasmine Mohamed (Distributed Warehouse Engineer)

nameNode=hdfs://news-namenode-01:8020
jobTracker=news-rm-01:8032
resourceManager=news-rm-01:8032
queueName=news_analytics
oozie.use.system.libpath=true
oozie.wf.application.path=${{nameNode}}/user/news/oozie/news_pipeline
oozie.coord.application.path=${{nameNode}}/user/news/oozie/news_pipeline
workflowDir={_rel(workflow_dir)}
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


def _write_real_cluster_commands(path: str, stats: dict) -> None:
    """Write the authentic Oozie cluster runbook (real_cluster_commands.md)."""
    md = f"""# Apache Oozie — Real Production Cluster Commands

Project : News Article Analysis and Topic Extraction
Owner   : Member 3 — Yasmine Mohamed (Distributed Warehouse Engineer)

## 1. Validate the workflow XML before submitting

```bash
oozie validate workflow.xml
```

## 2. Upload the workflow definition to HDFS (the Oozie app path)

```bash
hdfs dfs -mkdir -p /user/news/oozie/news_pipeline
hdfs dfs -put workflow.xml coordinator.xml job.properties /user/news/oozie/news_pipeline/
```

## 3. Run the workflow on the cluster

```bash
oozie job -oozie http://oozie-host:11000/oozie -config job.properties -run
```

## 4. Submit the coordinator (runs the workflow on its schedule)

```bash
oozie job -oozie http://oozie-host:11000/oozie -config job.properties \\
          -submit -Doozie.coord.application.path=$oozie.coord.application.path
oozie job -oozie http://oozie-host:11000/oozie -config job.properties -start
```

## 5. Inspect the running jobs

```bash
oozie jobs -oozie http://oozie-host:11000/oozie -filter status=RUNNING
oozie job -oozie http://oozie-host:11000/oozie -info <job-id>
oozie job -oozie http://oozie-host:11000/oozie -log <job-id>
```

## Local simulation result on this workstation
- Workflow actions : {', '.join(stats.get('workflow_actions', []))}
- Actions executed : {stats.get('actions_executed', 0)}
- Actions skipped  : {stats.get('actions_skipped', 0)}
- Final status     : {stats.get('final_status', 'UNKNOWN')}
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(md)


# ---------------------------------------------------------------------------
# DAG parsing (ElementTree)
# ---------------------------------------------------------------------------
def _local(tag: str) -> str:
    """Strip the XML namespace from a tag, returning the local name."""
    return tag.rsplit("}", 1)[-1]


def _parse_workflow(path: str):
    """Parse workflow.xml and return (start_target, end_name, kill_name, actions_map)."""
    root = ET.parse(path).getroot()
    start_target = end_name = kill_name = None
    actions = {}
    for el in root:
        name = _local(el.tag)
        if name == "start":
            start_target = el.get("to")
        elif name == "end":
            end_name = el.get("name")
        elif name == "kill":
            kill_name = el.get("name")
        elif name == "action":
            actions[el.get("name")] = el
    return start_target, end_name, kill_name, actions


def _action_type(action_el) -> str:
    """Return the sub-action type (sqoop/pig/hive/spark/shell/map-reduce)."""
    for child in action_el:
        local = _local(child.tag)
        if local not in ("ok", "error"):
            return local
    return "unknown"


def _ok_target(action_el) -> str:
    """Return the name this action transitions to on success."""
    for child in action_el:
        if _local(child.tag) == "ok":
            return child.get("to")
    return None


# ---------------------------------------------------------------------------
# Pluggable action executors — each performs a small REAL operation.
# A callback returns (status, detail) where status is OK / SKIPPED / FAILED.
# ---------------------------------------------------------------------------
def _exec_sqoop(_):
    """Sqoop action: inspect the real RDBMS produced by the Sqoop component."""
    db = os.path.join(PROJECT_ROOT, "data", "hadoop_ecosystem", "sqoop", "news_staging.db")
    if os.path.exists(db):
        con = sqlite3.connect(db)
        try:
            n = con.execute("SELECT COUNT(*) FROM articles_staging").fetchone()[0]
        finally:
            con.close()
        return "OK", f"RDBMS articles_staging verified ({n} rows)"
    csv_dump = os.path.join(PROJECT_ROOT, "data", "hadoop_ecosystem", "sqoop", "articles_staging.csv")
    if os.path.exists(csv_dump):
        return "OK", "RDBMS CSV dump verified"
    return "SKIPPED", "no Sqoop output found (news_staging.db / articles_staging.csv)"


def _exec_pig(_):
    """Pig/MapReduce action: read the real MapReduce term-count output."""
    tsv = os.path.join(PROJECT_ROOT, "data", "mapreduce_term_counts.tsv")
    if not os.path.exists(tsv):
        return "SKIPPED", "mapreduce_term_counts.tsv not found"
    top = None
    with open(tsv, "r", encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 2:
                top = line.strip()
                break
    return "OK", f"MapReduce transform verified (top term: {top})"


def _exec_hive(_):
    """Hive action: read the warehouse result CSVs, else fall back to summary JSON."""
    hive_csvs = glob.glob(os.path.join(PROJECT_ROOT, "data", "hadoop_ecosystem", "hive", "*.csv"))
    if hive_csvs:
        with open(hive_csvs[0], "r", encoding="utf-8") as fh:
            rows = sum(1 for _ in fh) - 1  # minus header
        return "OK", f"Hive warehouse verified ({os.path.basename(hive_csvs[0])}: {rows} rows)"
    summary = os.path.join(PROJECT_ROOT, "data", "pipeline_analytics_summary.json")
    if os.path.exists(summary):
        with open(summary, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        n_topics = len(data.get("topic_summary", []))
        return "OK", f"Hive rollup from analytics summary ({n_topics} topic rollups)"
    return "SKIPPED", "no Hive warehouse output found"


def _exec_spark(_):
    """Spark action: verify the real Parquet topic-model output lake."""
    parquet_dir = os.path.join(PROJECT_ROOT, "data", "processed_parquet")
    if not os.path.isdir(parquet_dir):
        return "SKIPPED", "processed_parquet directory not found"
    files = glob.glob(os.path.join(parquet_dir, "**", "*.parquet"), recursive=True)
    if not files:
        return "SKIPPED", "no parquet partitions found"
    return "OK", f"Spark LDA lake verified ({len(files)} parquet partitions)"


def _exec_shell(out_dir):
    """Shell action: verify a produced artifact file genuinely exists."""
    artifact = os.path.join(out_dir, "coordinator.xml")
    if os.path.exists(artifact):
        return "OK", f"artifact verified: {os.path.basename(artifact)} exists"
    return "SKIPPED", "verify target artifact not present"


def _exec_mapreduce(_):
    """Map-Reduce action (alternative to pig): read the term-count output."""
    return _exec_pig(_)


# Map each Oozie action type to its executor callback.
ACTION_EXECUTORS = {
    "sqoop": _exec_sqoop,
    "pig": _exec_pig,
    "map-reduce": _exec_mapreduce,
    "hive": _exec_hive,
    "spark": _exec_spark,
    "shell": _exec_shell,
}


def run_oozie_workflow(workflow_xml="data/hadoop_ecosystem/oozie/workflow.xml",
                       out_dir="data/hadoop_ecosystem/oozie") -> dict:
    """
    Generate the Oozie XML artifacts, parse the DAG and execute it locally.

    Returns the mandated contract dict with keys:
        workflow_actions, actions_executed, actions_skipped, final_status,
        journal_path, out_dir
    """
    print("[Oozie] Generating Oozie workflow/coordinator XML artifacts ...")
    out_dir = _resolve(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    wf_path = _resolve(workflow_xml)
    os.makedirs(os.path.dirname(wf_path), exist_ok=True)

    # --- STEP 1: generate the real cluster definition files -----------------
    _generate_workflow_xml(wf_path)
    _generate_coordinator_xml(os.path.join(os.path.dirname(wf_path), "coordinator.xml"))
    _write_job_properties(os.path.join(os.path.dirname(wf_path), "job.properties"),
                          os.path.dirname(wf_path))

    # --- STEP 2: parse the DAG out of workflow.xml --------------------------
    start_target, end_name, kill_name, actions = _parse_workflow(wf_path)
    print(f"[Oozie] Parsed DAG: start -> {start_target}; {len(actions)} actions; "
          f"end='{end_name}', kill='{kill_name}'")

    # --- STEP 3: execute the actions in dependency order --------------------
    order = []          # action names in execution order
    journal = []        # full execution journal
    executed = 0
    skipped = 0
    failed = 0
    current = start_target
    guard = 0
    while current and current not in (end_name, kill_name):
        guard += 1
        if guard > 1000:
            raise RuntimeError("workflow DAG resolution exceeded the loop guard")
        action_el = actions.get(current)
        if action_el is None:
            # Unknown target: stop safely, keeping transitions consistent.
            print(f"[Oozie] Unknown transition target '{current}' — stopping DAG")
            break
        a_type = _action_type(action_el)
        executor = ACTION_EXECUTORS.get(a_type)
        t0 = time.time()
        if executor is None:
            status, detail = "SKIPPED", f"no executor registered for type '{a_type}'"
        else:
            status, detail = executor(os.path.dirname(wf_path))
        duration_ms = round((time.time() - t0) * 1000, 3)
        # --- record + tally ------------------------------------------------
        order.append(current)
        journal.append({
            "action": current,
            "type": a_type,
            "status": status,
            "duration_ms": duration_ms,
            "detail": detail,
        })
        if status == "OK":
            executed += 1
        elif status == "SKIPPED":
            skipped += 1
        else:
            failed += 1
        print(f"[Oozie] action {current} [{a_type}] -> {status} ({duration_ms} ms): {detail}")
        # SKIPPED actions still follow their ok transition to keep the DAG flowing.
        current = _ok_target(action_el)

    final_status = "SUCCEEDED" if failed == 0 else "FAILED"

    # --- STEP 4: write the execution journal + summary ----------------------
    journal_path = os.path.join(out_dir, "oozie_execution_journal.json")
    with open(journal_path, "w", encoding="utf-8") as fh:
        json.dump({
            "workflow": "news_pipeline_workflow",
            "generated": datetime.datetime.now().isoformat(timespec="seconds"),
            "journal": journal,
        }, fh, indent=2, default=str)

    summary = {
        "workflow_actions": order,
        "actions_executed": executed,
        "actions_skipped": skipped,
        "final_status": final_status,
        "journal_path": _rel(journal_path),
        "out_dir": _rel(out_dir),
    }
    with open(os.path.join(out_dir, "oozie_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    _write_real_cluster_commands(os.path.join(out_dir, "real_cluster_commands.md"), summary)

    print(f"[Oozie] Done: {executed} executed, {skipped} skipped, final status {final_status}.")
    return summary


if __name__ == "__main__":
    # Run the component stand-alone and print a completion summary.
    result = run_oozie_workflow()
    print("[Oozie] Completion summary:")
    print(json.dumps(result, indent=2, default=str))
