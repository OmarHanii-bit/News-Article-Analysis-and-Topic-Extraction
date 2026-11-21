# Production-Equivalent Cluster Command Templates — Apache Hive

Project : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache Hive (SQL data warehouse over HDFS/Parquet)
Owner   : Member 2 — Priya Sharma (Hadoop Ecosystem & MapReduce Specialist)

These are **production-equivalent command templates**; the workload was executed
locally via the Python simulation in `src/hadoop/`.

## 0. Warehouse location configuration
```properties
# hive-site.xml (or --hiveconf on the CLI)
hive.metastore.warehouse.dir=hdfs:///user/hive/warehouse
```
By default a **managed** table's data lives under the warehouse directory;
our `news_articles` table is **EXTERNAL**, so its data stays in the Spark
output path `/user/news/lakehouse` (partitioned year=/month=).

## 1. Run the DDL to register the external Parquet table
```bash
hive -f data/hadoop_ecosystem/hive/news_analytics.hql
```

## 2. Run the analytical queries from an interactive shell
```bash
hive
hive> SOURCE data/hadoop_ecosystem/hive/news_analytics.hql;
```

## 3. Beeline (JDBC / HiveServer2) instead of the classic hive CLI
```bash
beeline -u "jdbc:hive2://namenode:10000/default;principal=hive/_HOST@REALM" \
  -f data/hadoop_ecosystem/hive/news_analytics.hql
```

## 4. Repair the partition metadata after new Parquet partitions arrive
```bash
hive -e "MSCK REPAIR TABLE news_articles;"
```

## 5. Refresh the CBO statistics
```bash
hive -e "ANALYZE TABLE news_articles COMPUTE STATISTICS;"
hive -e "ANALYZE TABLE news_articles COMPUTE STATISTICS FOR COLUMNS topic_label, publication;"
```

## 6. Register the MapReduce TSV and run the join
```bash
hive -f data/hadoop_ecosystem/hive/topic_term_hive.hql
```

## 7. Explain a partition-pruned query (proves pruning in the plan)
```bash
hive -e "EXPLAIN SELECT COUNT(*) FROM news_articles WHERE year=2025 AND month=12;"
```

