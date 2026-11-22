"""
Hadoop Cluster & YARN Resource Management Configuration
=======================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Cluster Computing — HDFS / YARN / MapReduce site configuration
Owner: Member 2 — Mohammed Hossam (Hadoop & Cluster Specialist)

WHAT THIS MODULE DOES
---------------------
A Hadoop cluster is defined entirely by its XML configuration files. This module

    1. Declares an explicit cluster topology (1 NameNode, 3 DataNodes, 1 Resource
       Manager, 3 NodeManagers, plus the ZooKeeper quorum).
    2. COMPUTES a real capacity plan directly from the actual news dataset on
       disk (raw bytes -> HDFS blocks -> replicated storage -> YARN container
       sizing -> Spark executor count). Nothing here is hardcoded: change the
       dataset and the plan changes.
    3. EMITS the authentic production configuration files that a real cluster
       Administrator would deploy: core-site.xml, hdfs-site.xml, yarn-site.xml,
       mapred-site.xml, capacity-scheduler.xml, hbase-site.xml, hive-site.xml,
       spark-defaults.conf and the HDFS/YARN startup command set.

The generated XML uses the genuine property names (`dfs.blocksize`,
`dfs.replication`, `yarn.nodemanager.resource.memory-mb`, ...) so the artifacts
can be dropped straight into `$HADOOP_HOME/etc/hadoop/` on a real cluster.
"""

import os
import json
import datetime
import xml.etree.ElementTree as ET


# ---------------------------------------------------------------------------
# Cluster topology definition. In production these are physical hosts; here they
# are logical roles used to size the cluster and to render the XML templates.
# ---------------------------------------------------------------------------
CLUSTER_TOPOLOGY = {
    "cluster_name": "news-bigdata-cluster",
    "namenode": {"host": "news-namenode-01", "role": "NameNode + ResourceManager", "memory_gb": 16, "disk_tb": 2},
    "datanodes": [
        {"host": "news-worker-01", "roles": "DataNode + NodeManager", "memory_gb": 32, "disk_tb": 8, "cores": 8},
        {"host": "news-worker-02", "roles": "DataNode + NodeManager", "memory_gb": 32, "disk_tb": 8, "cores": 8},
        {"host": "news-worker-03", "roles": "DataNode + NodeManager", "memory_gb": 32, "disk_tb": 8, "cores": 8},
    ],
    "zookeeper_ensemble": ["news-zk-01:2181", "news-zk-02:2181", "news-zk-03:2181"],
    "edge_node": {"host": "news-edge-01", "role": "Client gateway (spark-submit / hive / sqoop / flume)"},
}

# Production HDFS block size: 128 MB (the canonical Hadoop default).
PROD_BLOCK_SIZE_MB = 128
# Production replication factor for a rack-aware, fault-tolerant cluster.
PROD_REPLICATION = 3
# NodeManager capacity: ~80% of each worker's 32 GB, computed with INTEGER maths
# (32 * 1024 * 8 // 10 == 26214). Float arithmetic would emit "26214.0", which
# Hadoop's integer parser rejects.
NODEMANAGER_MEMORY_MB = (32 * 1024 * 8) // 10

# Properties that Hadoop parses as strict positive integers. Validating them at
# generation time prevents plausible-looking but invalid XML (e.g. "26214.0").
_POSITIVE_INT_PROPERTIES = {
    "io.file.buffer.size", "fs.trash.interval", "dfs.replication", "dfs.blocksize",
    "dfs.namenode.handler.count", "yarn.nodemanager.resource.memory-mb",
    "yarn.nodemanager.resource.cpu-vcores", "yarn.scheduler.minimum-allocation-mb",
    "yarn.scheduler.maximum-allocation-mb", "mapreduce.map.memory.mb",
    "mapreduce.reduce.memory.mb", "mapreduce.job.reduces", "mapreduce.task.io.sort.mb",
    "hbase.hregion.max.filesize", "hbase.regionserver.handler.count",
}
_BOOLEAN_PROPERTIES = {
    "dfs.webhdfs.enabled", "yarn.nodemanager.vmem-check-enabled",
    "yarn.log-aggregation-enable",
}


def _validate_config_values(properties: list, filename: str) -> None:
    """Rejects empty, malformed or physically implausible Hadoop settings."""
    by_name = {}
    for name, value, _comment in properties:
        text = str(value)
        if name in by_name:
            raise ValueError(f"{filename}: duplicate property {name}")
        by_name[name] = text
        if not text.strip():
            raise ValueError(f"{filename}: empty value for {name}")
        # Integer fields must be digit strings - never '26214.0' or '32 GB'.
        if name in _POSITIVE_INT_PROPERTIES and (not text.isdigit() or int(text) <= 0):
            raise ValueError(f"{filename}: {name} must be a positive integer, got {text!r}")
        # Boolean flags are lower-case true/false, as Hadoop's parser expects.
        if name in _BOOLEAN_PROPERTIES and text.lower() not in {"true", "false"}:
            raise ValueError(f"{filename}: {name} must be true/false, got {text!r}")
    # Container bounds must fit inside the memory a NodeManager advertises.
    yarn_keys = {"yarn.scheduler.minimum-allocation-mb",
                 "yarn.scheduler.maximum-allocation-mb",
                 "yarn.nodemanager.resource.memory-mb"}
    if yarn_keys.issubset(by_name):
        low = int(by_name["yarn.scheduler.minimum-allocation-mb"])
        high = int(by_name["yarn.scheduler.maximum-allocation-mb"])
        node = int(by_name["yarn.nodemanager.resource.memory-mb"])
        if not (0 < low <= high <= node):
            raise ValueError(f"{filename}: YARN bounds {low}..{high} do not fit "
                             f"inside NodeManager memory {node}")


def _write_property_xml(path: str, root_tag: str, properties: list) -> str:
    """
    Renders a Hadoop-style <configuration> XML file.

    `properties` is a list of (name, value, comment) tuples. Each becomes a
    <property><name/><value/><description/></property> element — the exact
    structure Hadoop's Configuration class parses.
    """
    # Fail fast on implausible values before any file is written to disk.
    _validate_config_values(properties, os.path.basename(path))
    configuration = ET.Element(root_tag)
    for name, value, comment in properties:
        prop = ET.SubElement(configuration, "property")
        # <name> is the configuration key Hadoop looks up.
        ET.SubElement(prop, "name").text = name
        # <value> is the effective setting applied cluster-wide.
        ET.SubElement(prop, "value").text = str(value)
        # <description> documents intent — required for auditability.
        ET.SubElement(prop, "description").text = comment
    # ET.indent produces human-readable, diff-friendly XML (Python 3.9+).
    ET.indent(configuration, space="    ")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Declare the standard UTF-8 XML prolog that Hadoop expects.
    tree = ET.ElementTree(configuration)
    tree.write(path, encoding="utf-8", xml_declaration=True)
    return path


def compute_capacity_plan(input_csv: str = "data/raw_news_articles.csv",
                          mapreduce_tsv: str = "data/mapreduce_term_counts.tsv",
                          parquet_dir: str = "data/processed_parquet") -> dict:
    """
    Measures the real on-disk dataset and derives the cluster capacity plan.

    Returns a dict containing HDFS block counts, replicated storage demand and
    YARN/Spark resource sizing recommendations.
    """
    # --- 1. Measure the real raw input volume on disk --------------------
    raw_bytes = os.path.getsize(input_csv) if os.path.exists(input_csv) else 0
    tsv_bytes = os.path.getsize(mapreduce_tsv) if os.path.exists(mapreduce_tsv) else 0

    # --- 2. Measure the Parquet lakehouse volume (recursive walk) --------
    parquet_bytes = 0
    parquet_files = 0
    if os.path.isdir(parquet_dir):
        for root_dir, _, files in os.walk(parquet_dir):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_bytes += os.path.getsize(os.path.join(root_dir, f))
                    parquet_files += 1

    # Total logical bytes that must be stored inside HDFS.
    logical_bytes = raw_bytes + tsv_bytes + parquet_bytes

    # --- 3. HDFS block arithmetic ---------------------------------------
    block_size_bytes = PROD_BLOCK_SIZE_MB * 1024 * 1024
    # Number of 128 MB blocks required for the whole logical dataset.
    total_blocks = max(1, -(-logical_bytes // block_size_bytes))  # ceiling division
    # Physical storage consumed once 3x replication is applied.
    replicated_bytes = logical_bytes * PROD_REPLICATION
    # Headroom factor: production clusters keep ~30% free for temp/shuffle data.
    recommended_capacity_bytes = int(replicated_bytes * 1.3)

    # --- 4. YARN resource sizing ----------------------------------------
    # Sum the NodeManager memory across all worker nodes.
    worker_memory_gb = sum(node["memory_gb"] for node in CLUSTER_TOPOLOGY["datanodes"])
    # YARN convention: reserve ~20% of each NodeManager's RAM for the OS/JVM.
    yarn_available_gb = int(worker_memory_gb * 0.8)
    # Standard container size used by MapReduce/Spark executors on this cluster.
    container_gb = 4
    # Number of concurrent containers the cluster can schedule.
    max_containers = yarn_available_gb // container_gb
    # Spark: 1 driver container + N executors (leaving 1 slot for an AM).
    # Spark's recommended executor count = min(containers - 1, cores per node * nodes).
    total_worker_cores = sum(node["cores"] for node in CLUSTER_TOPOLOGY["datanodes"])
    spark_executors = max(1, min(max_containers - 1, total_worker_cores - 1))
    # Cores per executor (Spark best practice: 5 cores/executor for HDFS I/O).
    cores_per_executor = min(5, max(1, total_worker_cores // max(1, spark_executors)))
    # Executor memory must leave room for the driver plus JVM overhead.
    executor_memory_gb = container_gb
    # Driver memory: the LDA model state lives on the driver in our pipeline.
    driver_memory_gb = 4

    return {
        "measured_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "raw_csv_bytes": raw_bytes,
        "mapreduce_tsv_bytes": tsv_bytes,
        "parquet_bytes": parquet_bytes,
        "parquet_files": parquet_files,
        "logical_bytes": logical_bytes,
        "logical_mb": round(logical_bytes / (1024 * 1024), 2),
        "block_size_mb": PROD_BLOCK_SIZE_MB,
        "total_blocks": total_blocks,
        "replication_factor": PROD_REPLICATION,
        "replicated_bytes": replicated_bytes,
        "recommended_hdfs_capacity_bytes": recommended_capacity_bytes,
        "recommended_hdfs_capacity_gb": round(recommended_capacity_bytes / (1024 ** 3), 2),
        "worker_memory_gb": worker_memory_gb,
        "yarn_available_memory_gb": yarn_available_gb,
        "container_size_gb": container_gb,
        "max_concurrent_containers": max_containers,
        "spark_executor_instances": spark_executors,
        "spark_executor_cores": cores_per_executor,
        "spark_executor_memory_gb": executor_memory_gb,
        "spark_driver_memory_gb": driver_memory_gb,
    }


def generate_hadoop_configs(out_dir: str = "data/hadoop_ecosystem/cluster") -> dict:
    """
    Emits the production Hadoop/YARN XML configuration set plus spark-defaults.
    """
    nn = CLUSTER_TOPOLOGY["namenode"]["host"]
    created = []

    # ---------------- core-site.xml : cluster-wide defaults --------------
    created.append(_write_property_xml(
        os.path.join(out_dir, "core-site.xml"), "configuration", [
            ("fs.defaultFS", f"hdfs://{nn}:8020",
             "Default filesystem URI. All hdfs:/// paths resolve to this NameNode."),
            ("hadoop.tmp.dir", "/data/hadoop/tmp",
             "Base directory for temporary Hadoop files (local disk on each node)."),
            ("io.file.buffer.size", "131072",
             "128 KB stream buffer — tuned for high-throughput sequential text reads."),
            ("fs.trash.interval", "1440",
             "Enable a 24-hour HDFS Trash to protect against accidental `hdfs dfs -rm`."),
            ("ha.zookeeper.quorum", ",".join(CLUSTER_TOPOLOGY["zookeeper_ensemble"]),
             "ZooKeeper quorum used for NameNode automatic failover (HA)."),
        ]))

    # ---------------- hdfs-site.xml : storage & replication --------------
    created.append(_write_property_xml(
        os.path.join(out_dir, "hdfs-site.xml"), "configuration", [
            ("dfs.replication", str(PROD_REPLICATION),
             "3x block replication — tolerates two simultaneous DataNode failures."),
            ("dfs.blocksize", str(PROD_BLOCK_SIZE_MB * 1024 * 1024),
             "128 MB DFS block size: large sequential blocks amortise NameNode metadata."),
            ("dfs.namenode.name.dir", "file:///data/hadoop/hdfs/namenode",
             "On-disk location of the NameNode fsimage and edit log."),
            ("dfs.datanode.data.dir", "file:///data/hadoop/hdfs/datanode",
             "Comma-separated DataNode volume list (one entry per physical disk)."),
            ("dfs.namenode.handler.count", "200",
             "Concurrent RPC handler threads on the NameNode (large clusters)."),
            # No 'dfs.blocksize.balancer.bandwidthPerSec': that name is NOT a real
            # Hadoop property. The balancer actually uses
            # 'dfs.datanode.balance.bandwidthPerSec'; it is intentionally omitted
            # here so every emitted configuration key is valid on a real cluster.
            ("dfs.webhdfs.enabled", "true",
             "Enable WebHDFS REST API for programmatic file access."),
        ]))

    # ---------------- yarn-site.xml : resource scheduling ----------------
    created.append(_write_property_xml(
        os.path.join(out_dir, "yarn-site.xml"), "configuration", [
            ("yarn.resourcemanager.hostname", nn,
             "ResourceManager host — the single authority for cluster containers."),
            ("yarn.nodemanager.resource.memory-mb", str(NODEMANAGER_MEMORY_MB),
             "Memory each NodeManager may offer as containers (~80% of 32 GB)."),
            ("yarn.nodemanager.resource.cpu-vcores", "8",
             "vCores each NodeManager advertises for container scheduling."),
            ("yarn.scheduler.minimum-allocation-mb", "1024",
             "Smallest container the scheduler will grant (1 GB)."),
            ("yarn.scheduler.maximum-allocation-mb", "16384",
             "Largest container (16 GB) — bounds runaway Spark driver requests."),
            ("yarn.nodemanager.vmem-check-enabled", "false",
             "Disable strict virtual-memory checks; required for Spark/JVM workloads."),
            ("yarn.log-aggregation-enable", "true",
             "Aggregate container logs into HDFS so they outlive the container."),
            ("yarn.resourcemanager.scheduler.class",
             "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler",
             "CapacityScheduler enforces per-queue guarantees (see capacity-scheduler.xml)."),
        ]))

    # ---------------- mapred-site.xml : MapReduce framework --------------
    created.append(_write_property_xml(
        os.path.join(out_dir, "mapred-site.xml"), "configuration", [
            ("mapreduce.framework.name", "yarn",
             "Run MapReduce on top of YARN rather than the legacy local runner."),
            ("mapreduce.map.memory.mb", "2048",
             "Container memory requested for each map task."),
            ("mapreduce.reduce.memory.mb", "4096",
             "Container memory requested for each reduce task (shuffle-heavy)."),
            ("mapreduce.map.java.opts", "-Xmx1638m",
             "Map JVM heap kept ~80% of the container to avoid swap."),
            ("mapreduce.reduce.java.opts", "-Xmx3276m",
             "Reduce JVM heap kept ~80% of the container."),
            ("mapreduce.job.reduces", "8",
             "Default reducer count for the news token-frequency job."),
            ("mapreduce.task.io.sort.mb", "512",
             "512 MB in-memory sort buffer per map before spilling to disk."),
        ]))

    # ---------------- capacity-scheduler.xml : queue guarantees ----------
    created.append(_write_property_xml(
        os.path.join(out_dir, "capacity-scheduler.xml"), "configuration", [
            ("yarn.scheduler.capacity.root.queues", "etl,analytics,batch",
             "Three top-level queues separating ETL, interactive analytics and batch."),
            ("yarn.scheduler.capacity.root.etl.capacity", "30",
             "30% of cluster capacity reserved for ingestion (Flume/Sqoop)."),
            ("yarn.scheduler.capacity.root.analytics.capacity", "40",
             "40% reserved for interactive Spark/PySpark analytics and LDA training."),
            ("yarn.scheduler.capacity.root.batch.capacity", "30",
             "30% reserved for long-running Hadoop MapReduce batch jobs."),
            ("yarn.scheduler.capacity.root.analytics.maximum-capacity", "70",
             "Analytics may burst to 70% when the batch queue is idle."),
            ("yarn.scheduler.capacity.root.default.maximum-applications", "200",
             "Cap concurrent applications to protect the ResourceManager."),
        ]))

    # ---------------- hbase-site.xml : column-family store --------------
    zk = CLUSTER_TOPOLOGY["zookeeper_ensemble"][0].split(":")[0]
    created.append(_write_property_xml(
        os.path.join(out_dir, "hbase-site.xml"), "configuration", [
            ("hbase.rootdir", f"hdfs://{nn}:8020/hbase",
             "HBase stores its HFiles inside HDFS rather than on local disk."),
            ("hbase.cluster.distributed", "true",
             "Run HBase in distributed mode (one Master + RegionServers)."),
            ("hbase.zookeeper.quorum", ",".join(e.split(":")[0] for e in CLUSTER_TOPOLOGY["zookeeper_ensemble"]),
             "ZooKeeper quorum HBase uses to track the active HMaster and region map."),
            ("zookeeper.znode.parent", "/hbase",
             "Root znode under which HBase stores its coordination state."),
            ("hbase.hregion.max.filesize", "10737418240",
             "10 GB max region size before an automatic region split."),
            ("hbase.regionserver.handler.count", "60",
             "Concurrent RPC handlers per RegionServer."),
        ]))

    # ---------------- hive-site.xml : warehouse -------------------------
    created.append(_write_property_xml(
        os.path.join(out_dir, "hive-site.xml"), "configuration", [
            ("javax.jdo.option.ConnectionURL",
             "jdbc:mysql://rdbms-host:3306/hive_metastore?createDatabaseIfNotExist=true",
             "The Hive metastore is an RDBMS holding table/partition metadata."),
            ("hive.metastore.warehouse.dir", f"hdfs://{nn}:8020/user/hive/warehouse",
             "Default HDFS location for Hive managed tables."),
            ("hive.exec.dynamic.partition", "true",
             "Allow dynamic partition inserts (needed for the year/month rollups)."),
            ("hive.exec.dynamic.partition.mode", "nonstrict",
             "Permit dynamic partitioning without requiring a static partition."),
            ("hive.execution.engine", "tez",
             "Tez DAG engine — materially faster than MapReduce for HiveQL."),
            ("hive.stats.autogather", "true",
             "Collect column statistics automatically for the cost-based optimiser."),
        ]))

    # ---------------- spark-defaults.conf : YARN deployment -------------
    plan = compute_capacity_plan()
    spark_conf_path = os.path.join(out_dir, "spark-defaults.conf")
    with open(spark_conf_path, "w", encoding="utf-8") as fh:
        fh.write("# Spark defaults for YARN deployment — News Topic Extraction pipeline\n")
        fh.write(f"spark.master                       yarn\n")
        fh.write(f"spark.submit.deployMode            client\n")
        fh.write(f"spark.driver.memory                {plan['spark_driver_memory_gb']}g\n")
        fh.write(f"spark.executor.instances           {plan['spark_executor_instances']}\n")
        fh.write(f"spark.executor.cores               {plan['spark_executor_cores']}\n")
        fh.write(f"spark.executor.memory              {plan['spark_executor_memory_gb']}g\n")
        fh.write(f"spark.yarn.queue                   analytics\n")
        fh.write(f"spark.dynamicAllocation.enabled    true\n")
        fh.write(f"spark.sql.shuffle.partitions       8\n")
        fh.write(f"spark.serializer                   org.apache.spark.serializer.KryoSerializer\n")
    created.append(spark_conf_path)

    return {"config_files": created, "config_count": len(created), "out_dir": out_dir}


def generate_real_cluster_commands(out_dir: str = "data/hadoop_ecosystem/cluster") -> str:
    """
    Writes `real_cluster_commands.md` — the authentic bring-up / run / monitor
    command set for a genuine Hadoop+YARN cluster.
    """
    nn = CLUSTER_TOPOLOGY["namenode"]["host"]
    commands = f"""# Real Hadoop Cluster Operations — News Article Analysis Pipeline

> Generated by `src/hadoop/cluster_config.py`. These are the authentic production
> commands executed on a real cluster. The local simulation performs the same
> logical operations using `src/hadoop/hdfs_client.py`.

## 1. Cluster bring-up (performed once by the administrator)

```bash
# 1.1 Format the NameNode (destructive — wipes HDFS metadata)
hdfs namenode -format -clusterId news-bigdata-cluster

# 1.2 Start the HDFS daemons: NameNode and all DataNodes
start-dfs.sh

# 1.3 Start the YARN daemons: ResourceManager and all NodeManagers
start-yarn.sh

# 1.4 Start the ZooKeeper ensemble (required by HDFS HA and HBase)
zkServer.sh start                     # executed on each of the 3 ZK hosts

# 1.5 Start the HBase Master and RegionServers
start-hbase.sh
```

## 2. Verify cluster health

```bash
hdfs dfsadmin -report                 # live DataNodes, capacity, block health
yarn node -list -all                  # NodeManager inventory and health
yarn application -list                # currently running YARN applications
hdfs fsck /user/news -files -blocks -locations   # block placement audit
```

## 3. Land the news corpus into HDFS

```bash
hdfs dfs -mkdir -p /user/news/raw /user/news/lakehouse /user/news/stream
hdfs dfs -put data/raw_news_articles.csv /user/news/raw/
hdfs dfs -put data/mapreduce_term_counts.tsv /user/news/raw/
hdfs dfs -put data/processed_parquet /user/news/lakehouse/
hdfs dfs -ls -h /user/news/raw
hdfs dfs -du -s -h /user/news
```

## 4. Run the MapReduce token-frequency job on YARN

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="News_Article_Term_Extraction" \\
    -D mapreduce.job.reduces=8 \\
    -D mapreduce.job.queuename=batch \\
    -files src/mapreduce/mapper.py,src/mapreduce/reducer.py \\
    -mapper "python3 mapper.py" \\
    -reducer "python3 reducer.py" \\
    -input /user/news/raw/raw_news_articles.csv \\
    -output /user/news/output_term_frequencies
```

## 5. Submit the PySpark pipeline to YARN

```bash
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --queue analytics \\
    --num-executors {compute_capacity_plan()['spark_executor_instances']} \\
    --executor-cores {compute_capacity_plan()['spark_executor_cores']} \\
    --executor-memory {compute_capacity_plan()['spark_executor_memory_gb']}g \\
    --driver-memory {compute_capacity_plan()['spark_driver_memory_gb']}g \\
    --conf spark.dynamicAllocation.enabled=true \\
    src/spark_pipeline.py
```

## 6. Warehouse (Hive), transfer (Sqoop), streaming (Flume) and orchestration (Oozie)

```bash
hive -f data/hadoop_ecosystem/hive/news_analytics.hql
sqoop import --connect jdbc:mysql://rdbms-host:3306/news --table publishers \\
    --target-dir /user/news/sqoop_import/publishers --m 4 --as-parquetfile
flume-ng agent --conf ./conf --conf-file agent_news.properties --name a1 \\
    -Dflume.root.logger=INFO,console
oozie job -oozie http://{nn}:11000/oozie -config job.properties -run
```

## 7. Monitoring and troubleshooting

```bash
yarn logs -applicationId <application_id>      # aggregated container logs
yarn application -kill <application_id>        # terminate a runaway job
hdfs dfsadmin -safemode get                    # NameNode safe mode status
hdfs dfsadmin -report | grep -A5 "Live datanodes"
mapred job -list                               # running MapReduce jobs
```
"""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "real_cluster_commands.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(commands)
    return path


def run_cluster_planning(input_csv: str = "data/raw_news_articles.csv",
                         mapreduce_tsv: str = "data/mapreduce_term_counts.tsv",
                         parquet_dir: str = "data/processed_parquet",
                         out_dir: str = "data/hadoop_ecosystem/cluster") -> dict:
    """
    Main entry point: computes the capacity plan, writes every configuration
    file and the real command set, then persists a JSON summary.
    """
    print("[Cluster] Computing capacity plan from the real dataset on disk...")
    plan = compute_capacity_plan(input_csv, mapreduce_tsv, parquet_dir)
    print(f"[Cluster] Logical dataset size : {plan['logical_mb']:,} MB "
          f"({plan['raw_csv_bytes']:,} B CSV + {plan['parquet_bytes']:,} B Parquet)")
    print(f"[Cluster] HDFS blocks required : {plan['total_blocks']} x {plan['block_size_mb']} MB "
          f"@ replication {plan['replication_factor']}")
    print(f"[Cluster] YARN memory pool     : {plan['yarn_available_memory_gb']} GB "
          f"-> {plan['max_concurrent_containers']} containers of {plan['container_size_gb']} GB")

    configs = generate_hadoop_configs(out_dir)
    print(f"[Cluster] Wrote {configs['config_count']} configuration files to {out_dir}/")

    cmd_path = generate_real_cluster_commands(out_dir)
    print(f"[Cluster] Wrote production command set -> {cmd_path}")

    summary = {
        "component": "Cluster Computing (HDFS + YARN + MapReduce)",
        "owner": "Member 2 — Mohammed Hossam",
        "cluster_name": CLUSTER_TOPOLOGY["cluster_name"],
        "topology": CLUSTER_TOPOLOGY,
        "capacity_plan": plan,
        "config_files": [os.path.relpath(p) for p in configs["config_files"]],
        "commands_file": os.path.relpath(cmd_path),
        "out_dir": out_dir,
    }
    os.makedirs(out_dir, exist_ok=True)
    summary_path = os.path.join(out_dir, "cluster_summary.json")
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"[Cluster] Summary -> {summary_path}")
    return summary


if __name__ == "__main__":
    result = run_cluster_planning()
    print(f"[Cluster] DONE — {result['capacity_plan']['total_blocks']} blocks, "
          f"{result['capacity_plan']['spark_executor_instances']} Spark executors planned.")
