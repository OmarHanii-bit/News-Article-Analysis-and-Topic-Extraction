-- ==========================================================================
--  02_topic_publisher_join.pig
--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)
--  Component: Apache Pig (Pig Latin ETL)
--  Owner  : Member 2 — Mohammed Hossam (Hadoop & MapReduce Specialist)
--  Run    : pig -x mapreduce 02_topic_publisher_join.pig
-- ==========================================================================
--  GOAL: join publisher-level statistics with topic-level statistics to build
--        a per-article report enriched with both aggregates.

-- 1) LOAD the staged enriched corpus exported from the Parquet lakehouse.
enriched = LOAD 'data/hadoop_ecosystem/pig/staging/enriched_articles.csv' USING PigStorage(',') AS (
    article_id:chararray, publication:chararray, topic_label:chararray,
    sentiment_polarity:double, word_count:int);

-- 2) FILTER: keep only labelled articles with a real sentiment score.
valid = FILTER enriched BY topic_label != '' AND sentiment_polarity IS NOT NULL;

-- 3) Publisher-level statistics (GROUP by publication).
pub_groups = GROUP valid BY publication;
publisher_stats = FOREACH pub_groups GENERATE
    group AS pub,
    COUNT(valid) AS pub_articles,
    AVG(valid.sentiment_polarity) AS pub_avg_sent;

-- 4) Topic-level statistics (GROUP by topic).
topic_groups = GROUP valid BY topic_label;
topic_stats = FOREACH topic_groups GENERATE
    group AS tlabel,
    COUNT(valid) AS topic_articles,
    AVG(valid.sentiment_polarity) AS topic_avg_sent;

-- 5) JOIN articles->publisher stats, then ->topic stats.
pub_joined = JOIN valid BY publication, publisher_stats BY pub;
full_joined = JOIN pub_joined BY topic_label, topic_stats BY tlabel;

-- 6) Project the final report columns.
report = FOREACH full_joined GENERATE
    article_id, publication, topic_label,
    pub_articles, pub_avg_sent, topic_articles, topic_avg_sent;

-- 7) ORDER and keep the first 50 rows for readability.
ordered = ORDER report BY topic_label ASC, article_id ASC;
top50 = LIMIT ordered 50;

-- 8) STORE + DUMP.
STORE top50 INTO 'data/hadoop_ecosystem/pig/output/02_topic_publisher_join' USING PigStorage(',');
DUMP top50;

