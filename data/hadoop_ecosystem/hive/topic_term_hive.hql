-- ==========================================================================
--  topic_term_hive.hql
--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)
--  Component: Apache Hive data warehouse
--  Owner  : Member 3 — Yasmine Mohamed (Distributed Warehouse Engineer)
--  Purpose: Register the MapReduce term-frequency TSV as an external table
--           and join it with the LDA topic->term dimension.
--  Run    : hive -f topic_term_hive.hql
-- ==========================================================================

-- The MapReduce job writes tab-separated (term, count) lines to HDFS.
CREATE EXTERNAL TABLE IF NOT EXISTS term_counts (
    term STRING,
    cnt  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/news/raw/mapreduce_term_counts.tsv';

-- The topic->term dimension table (loaded from the LDA model output).
CREATE TABLE IF NOT EXISTS topics (
    topic_label STRING,
    topic_id    INT,
    term        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- Load the topic keyword mapping staged on the local filesystem.
LOAD DATA LOCAL INPATH 'data/hadoop_ecosystem/hive/topics.tsv'
OVERWRITE INTO TABLE topics;

-- Join the two tables to measure how strongly each topic's keywords appear
-- in the global term-frequency table produced by the MapReduce job.
SELECT t.topic_label,
       COUNT(DISTINCT tc.term) AS matched_terms,
       SUM(tc.cnt)             AS total_keyword_freq
FROM topics t
JOIN term_counts tc ON (t.term = tc.term)
GROUP BY t.topic_label
ORDER BY total_keyword_freq DESC;

