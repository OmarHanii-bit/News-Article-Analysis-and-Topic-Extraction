# Production-Equivalent Cluster Command Templates — Apache HBase

Project : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache HBase (column-family NoSQL store)
Owner   : Member 2 — Priya Sharma (Hadoop Ecosystem & MapReduce Specialist)

These are **production-equivalent command templates**; the workload was executed
locally via the Python simulation in `src/hadoop/` (`HBaseSimulator`). The templates
document the equivalent commands on a real HBase cluster.

## 0. Launch the interactive shell
```bash
hbase shell
```

## 1. Create the table with its three column families
```bash
create 'news_articles', {NAME=>'cf_meta', VERSIONS=>1, BLOOMFILTER=>'ROW', COMPRESSION=>'SNAPPY'}, {NAME=>'cf_nlp', VERSIONS=>1, BLOOMFILTER=>'ROW', COMPRESSION=>'SNAPPY'}, {NAME=>'cf_raw', VERSIONS=>1}
```

> Row key = `<salt>_<article_id>` (salt in `00..15`). The salt spreads writes
> across regions to avoid hotspotting; it also enables cheap prefix scans.
> Pre-split the key space into 16 regions:
```bash
hbase org.apache.hadoop.hbase.util.RegionSplitter news_articles HexStringSplit -c 16
```

## 2. Insert one row (put one cell at a time)
```bash
put 'news_articles', '08_ART_100008', 'cf_meta:title', 'Special Report: Emerging Trends in Quantum Computing and Market Dynamics'
put 'news_articles', '08_ART_100008', 'cf_nlp:topic_label', 'AI Infrastructure & Compute'
put 'news_articles', '08_ART_100008', 'cf_nlp:sentiment_polarity', '0.42'
put 'news_articles', '08_ART_100008', 'cf_raw:url', 'https://example.com/article'
```

## 3. Point lookups
```bash
get 'news_articles', '08_ART_100008'
get 'news_articles', '08_ART_100008', {COLUMN => 'cf_nlp:topic_label'}
```

## 4. Range and prefix scans
```bash
scan 'news_articles', {LIMIT => 5}
scan 'news_articles', {LIMIT => 5, COLUMNS => ['cf_meta:title', 'cf_nlp:topic_label']}
scan 'news_articles', {STARTROW => '00_', ENDROW => '00_~', LIMIT => 5}
```

## 5. Row count and table descriptor
```bash
count 'news_articles'
describe 'news_articles'
```

## 6. Bulk-load the CSV with the MapReduce ImportTsv tool
```bash
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.separator=, \
  -Dimporttsv.columns=HBASE_ROW_KEY,cf_meta:title,cf_meta:author,cf_meta:publication,
cf_meta:category,cf_meta:sub_topic,cf_raw:url,cf_raw:word_count \
  news_articles hdfs:///user/news/raw/raw_news_articles.csv
```

## 7. Complete the bulk load from staged HFiles
```bash
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/news/hbase_staging news_articles
```

