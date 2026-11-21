"""
Integration Tests: Hadoop Ecosystem Component Verification
==========================================================
Component: QA, Storage & Benchmarking Suite
Author: David Kim (QA, Benchmarking & Presentation Lead)

Verifies that the full Hadoop Ecosystem execution produced coherent evidence:
HDFS storage integrity, YARN/cluster configuration, and the analytical services
(Hive, HBase, Pig, Sqoop, Flume, Oozie, ZooKeeper).
"""

import unittest
import sys
import os
import json

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        self.assertTrue(os.path.exists(os.path.join(ECOSYSTEM_ROOT, "sqoop", "news_staging.db")))

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


if __name__ == "__main__":
    unittest.main()
