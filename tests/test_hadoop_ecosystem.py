"""
Integration Tests: Hadoop Ecosystem Component Verification
==========================================================
Component: QA, Storage & Benchmarking Suite
Author: Mazen Salem (QA, Benchmarking & Presentation Lead)

Verifies that the full Hadoop Ecosystem execution produced coherent evidence:
HDFS storage integrity, YARN/cluster configuration, and the analytical services
(Hive, HBase, Pig, Sqoop, Flume, Oozie, ZooKeeper).
"""

import unittest
import sys
import os
import json
import tempfile
import xml.etree.ElementTree as ET

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.hadoop.run_hadoop_ecosystem import _service_status
from src.hadoop.hive_analytics import run_hive_analytics
from src.hadoop.pig_transform import stage_enriched_csv

ECOSYSTEM_ROOT = "data/hadoop_ecosystem"
SUMMARY_PATH = os.path.join(ECOSYSTEM_ROOT, "hadoop_ecosystem_summary.json")


class TestHadoopEcosystem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(SUMMARY_PATH):
            raise unittest.SkipTest(
                f"Hadoop ecosystem summary missing: {SUMMARY_PATH}. "
                "Run `python src/hadoop/run_hadoop_ecosystem.py` first.")
        with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
            cls.summary = json.load(f)
        cls.components = cls.summary["components"]

    def test_all_nine_services_executed_without_failure(self):
        """All 9 ecosystem services must run and none may fail."""
        expected = {"hdfs", "cluster", "zookeeper", "sqoop", "flume",
                    "hbase", "hive", "pig", "oozie"}
        self.assertEqual(set(self.summary["services_executed"]), expected)
        self.assertEqual(self.summary["services_failed"], [])

    def test_hdfs_storage_integrity(self):
        """HDFS must hold files with 3x replication and zero missing blocks."""
        report = self.components["hdfs"]["dfs_report"]
        self.assertGreater(report["total_files"], 0)
        self.assertGreater(report["total_blocks"], 0)
        self.assertGreater(report["logical_bytes"], 0)
        self.assertEqual(report["configured_replication"], 3)
        self.assertEqual(report["live_datanodes"], 3)
        self.assertEqual(report["missing_blocks"], 0)
        self.assertEqual(report["under_replicated_blocks"], 0)
        # Physical footprint must equal logical bytes x replication.
        self.assertEqual(report["physical_bytes_with_replication"],
                         report["logical_bytes"] * 3)

    def test_hdfs_cli_commands_recorded(self):
        """The simulation must log the real `hdfs dfs` command equivalents it ran."""
        self.assertGreater(self.summary["hdfs_cli_commands_recorded"], 0)
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "hadoop_cli_commands.md")))

    def test_cluster_configuration_artifacts(self):
        """Cluster/YARN config files must be emitted with a capacity plan."""
        cluster = self.components["cluster"]
        self.assertTrue(cluster["capacity_plan"])
        expected_files = ["core-site.xml", "hdfs-site.xml", "yarn-site.xml",
                          "mapred-site.xml", "capacity-scheduler.xml",
                          "hbase-site.xml", "hive-site.xml", "spark-defaults.conf"]
        emitted = [os.path.basename(p) for p in cluster["config_files"]]
        for fname in expected_files:
            self.assertIn(fname, emitted, f"Missing cluster config: {fname}")
            self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "cluster", fname)))

    def test_zookeeper_coordination(self):
        """ZooKeeper must create znodes, register ephemeral members and elect a leader."""
        zk = self.components["zookeeper"]
        self.assertGreater(zk["znodes_created"], 0)
        self.assertGreater(zk["ephemeral_nodes"], 0)
        self.assertTrue(zk["leader_elected"])
        self.assertGreater(zk["watches_fired"], 0)
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "zookeeper", "znode_tree.json")))

    def test_sqoop_bulk_transfer(self):
        """Sqoop must import and export relational rows via HDFS."""
        sqoop = self.components["sqoop"]
        self.assertGreater(sqoop["records_imported"], 0)
        self.assertGreater(sqoop["records_exported"], 0)
        # The staging SQLite file is a regenerable intermediate (git-ignored);
        # the summary preserves its path and measured size.
        self.assertIn("news_staging.db", sqoop["rdbms"]["db_path"])
        self.assertGreater(sqoop["rdbms"]["size_bytes"], 0)

    def test_flume_streaming_ingestion(self):
        """Flume must stream events through its channel into HDFS."""
        flume = self.components["flume"]
        self.assertGreater(flume["events_ingested"], 0)
        self.assertGreater(flume["bytes_ingested"], 0)
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "flume", "agent_news.properties")))

    def test_hbase_column_family_store(self):
        """HBase must load rows across multiple column families / salted regions."""
        hbase = self.components["hbase"]
        self.assertGreater(hbase["rows_loaded"], 0)
        self.assertGreater(hbase["num_regions"], 0)
        self.assertGreaterEqual(len(hbase["column_families"]), 3)
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "hbase", "news_articles.json")))

    def test_hive_analytical_queries(self):
        """Hive must execute HiveQL analytical rollups and emit result CSVs."""
        hive = self.components["hive"]
        self.assertGreater(hive["queries_executed"], 0)
        self.assertGreater(hive["total_rows_scanned"], 0)
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "hive", "news_analytics.hql")))
        self.assertGreater(len(hive["result_files"]), 0)
        for csv_path in hive["result_files"]:
            self.assertTrue(os.path.exists(csv_path), f"Missing Hive result: {csv_path}")

    def test_pig_latin_etl(self):
        """Pig must execute its Pig Latin scripts and emit output part-files."""
        pig = self.components["pig"]
        self.assertGreater(pig["scripts_executed"], 0)
        self.assertGreater(pig["relations_created"], 0)
        self.assertGreater(len(pig["output_files"]), 0)
        for script in ("01_clean_and_count.pig", "02_topic_publisher_join.pig", "03_sentiment_filter.pig"):
            self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "pig", script)),
                            f"Missing Pig script: {script}")

    def test_oozie_workflow_succeeded(self):
        """The Oozie workflow DAG must complete with final status SUCCEEDED."""
        oozie = self.components["oozie"]
        self.assertEqual(oozie["final_status"], "SUCCEEDED")
        self.assertGreater(oozie["actions_executed"], 0)
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "oozie", "workflow.xml")))
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "oozie", "coordinator.xml")))

    def test_error_payloads_are_counted_as_failures(self):
        """An {'error': ...} component result must never be reported as executed."""
        executed, failed = _service_status({
            "hdfs": {"total_files": 3},
            "hive": {"error": "RuntimeError: boom"},
            "pig": {"error": "ValueError: bad input", "status": "FAILED"},
            "oozie": {"final_status": "SUCCEEDED"},
        })
        self.assertEqual(executed, ["hdfs", "oozie"])
        self.assertEqual(failed, ["hive", "pig"])
        # The persisted summary must agree with the same status helper.
        real_executed, real_failed = _service_status(self.components)
        self.assertEqual(set(self.summary["services_executed"]), set(real_executed))
        self.assertEqual(self.summary["services_failed"], real_failed)
        self.assertTrue(self.summary["all_services_succeeded"])
        self.assertTrue(self.summary["success"])

    def test_hive_no_fabricated_rows_when_inputs_absent(self):
        """Missing corpus -> empty results + warnings, never a synthetic row."""
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_hive_analytics(
                parquet_dir=os.path.join(tmp, "missing_parquet"),
                mapreduce_tsv=os.path.join(tmp, "missing_counts.tsv"),
                summary_json=os.path.join(tmp, "missing_summary.json"),
                out_dir=os.path.join(tmp, "hive"))
            self.assertFalse(summary["input_available"])
            self.assertEqual(summary["status"], "no_input_data")
            self.assertTrue(summary["warnings"])
            results_json = os.path.join(tmp, "hive", "hive_query_results.json")
            with open(results_json, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            self.assertEqual(payload["status"], "no_input_data")
            for query in payload["queries"]:
                self.assertEqual(query["status"], "no_input_data")
                self.assertEqual(query["row_count"], 0)
            for csv_path in summary["result_files"]:
                with open(csv_path, "r", encoding="utf-8") as fh:
                    body = fh.read()
                self.assertNotIn("Synthetic", body)
                self.assertNotIn("ART_SYNTH", body)

    def test_pig_no_fabricated_rows_when_inputs_absent(self):
        """Pig staging must not invent corpus rows when both inputs are missing."""
        with tempfile.TemporaryDirectory() as tmp:
            staged_path, staged_rows = stage_enriched_csv(
                parquet_dir=os.path.join(tmp, "missing_parquet"),
                input_csv=os.path.join(tmp, "missing.csv"),
                staging_dir=os.path.join(tmp, "staging"))
            self.assertEqual(staged_rows, 0)
            with open(staged_path, "r", encoding="utf-8") as fh:
                body = fh.read()
            self.assertEqual(body.strip(), "")  # headerless AND rowless
            self.assertNotIn("Synthetic", body)
            self.assertNotIn("ART_SYNTH", body)
        # Real part-files from the ecosystem run must also be placeholder-free.
        for part in self.components["pig"]["output_files"]:
            with open(part, "r", encoding="utf-8") as fh:
                self.assertNotIn("Synthetic", fh.read())

    def test_yarn_memory_config_is_integer_string(self):
        """Generated YARN memory must be an int string, and keys must be valid."""
        yarn = ET.parse(os.path.join(ECOSYSTEM_ROOT, "cluster", "yarn-site.xml")).getroot()
        values = {}
        for prop in yarn.findall("property"):
            values[prop.findtext("name")] = prop.findtext("value")
        memory = values["yarn.nodemanager.resource.memory-mb"]
        self.assertTrue(memory.isdigit(), f"Expected an int string, got {memory!r}")
        self.assertEqual(memory, str((32 * 1024 * 8) // 10))
        low = int(values["yarn.scheduler.minimum-allocation-mb"])
        high = int(values["yarn.scheduler.maximum-allocation-mb"])
        self.assertLessEqual(low, high)
        self.assertLessEqual(high, int(memory))
        hdfs_site = ET.parse(os.path.join(ECOSYSTEM_ROOT, "cluster", "hdfs-site.xml")).getroot()
        names = [p.findtext("name") for p in hdfs_site.findall("property")]
        self.assertNotIn("dfs.blocksize.balancer.bandwidthPerSec", names)

    def test_hive_ddl_locations_match_orchestrator(self):
        """Hive LOCATIONs must point at paths the orchestrator actually produces."""
        hive_dir = os.path.join(ECOSYSTEM_ROOT, "hive")
        with open(os.path.join(hive_dir, "news_analytics.hql"), "r", encoding="utf-8") as fh:
            ddl = fh.read()
        with open(os.path.join(hive_dir, "topic_term_hive.hql"), "r", encoding="utf-8") as fh:
            term_ddl = fh.read()
        self.assertIn("LOCATION '/user/news/lakehouse'", ddl)
        self.assertIn("LOCATION '/user/news/raw/mapreduce_term_counts.tsv'", term_ddl)
        self.assertNotIn("/user/news/processed/processed_parquet", ddl + term_ddl)
        self.assertNotIn("/user/news/mapreduce/term_counts", ddl + term_ddl)
        # The partition predicate is generated from the corpus, not hardcoded.
        partition = self.components["hive"]["partition"]
        self.assertIn(f"year = {partition['year']} AND month = {partition['month']}", ddl)


if __name__ == "__main__":
    unittest.main()
