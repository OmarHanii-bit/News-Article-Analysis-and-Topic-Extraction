# Production-Equivalent Cluster Command Templates — Apache Pig

Project : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache Pig (Pig Latin ETL compiled to MapReduce/Tez)
Owner   : Member 2 — Priya Sharma (Hadoop Ecosystem & MapReduce Specialist)

These are **production-equivalent command templates**; the workload was executed
locally via the Python simulation in `src/hadoop/` (`PigLatinEngine`). The templates
document the equivalent commands on a real Hadoop cluster.

## 0. Stage the scripts and the raw input onto HDFS
```bash
hdfs dfs -mkdir -p hdfs:///user/news/pig
hdfs dfs -put 01_clean_and_count.pig hdfs:///user/news/pig/
hdfs dfs -put raw_news_articles.csv hdfs:///user/news/raw/
```

## 1. Run a script in classic MapReduce mode
```bash
pig -x mapreduce 01_clean_and_count.pig
```

## 2. Run the same script on the Tez DAG engine (faster, no MR chaining)
```bash
pig -x tez 01_clean_and_count.pig
```

## 3. Tuning: enable map-side partial aggregation (combiner-like pre-agg)
```bash
pig -D pig.exec.mapPartAgg=true -D pig.exec.mapPartAgg.minReduction=0.8 \
    -x tez 01_clean_and_count.pig
```

## 4. Parameterise the run with -param (no script edits required)
```bash
pig -x tez -param INPUT=hdfs:///user/news/raw/raw_news_articles.csv \
    -param OUTPUT=hdfs:///user/news/pig/out 01_clean_and_count.pig
```

## 5. Run the full three-script pipeline
```bash
pig -x tez 01_clean_and_count.pig
pig -x tez 02_topic_publisher_join.pig
pig -x tez 03_sentiment_filter.pig
```

## 6. Pig on YARN — lifecycle notes
- Each Pig job runs as a YARN application; watch it with:
  `yarn application -list` / `yarn application -kill <app_id>`
- Use `pig -x local` for a single-JVM local run (no cluster, like this module).
- Logs / per-job counters: `pig -x tez -v 01_clean_and_count.pig`.

## 7. Inspect the stored output
```bash
hdfs dfs -ls hdfs:///user/news/pig/output
hdfs dfs -cat hdfs:///user/news/pig/output/01_word_counts/part-r-00000 | head
```

> Local part-file layout: outputs live in `data/hadoop_ecosystem/pig/output/<relation>/`
> with a `part-r-00000` data file and a `_SUCCESS` marker, mirroring HDFS.

