-- ==========================================================================
--  03_sentiment_filter.pig
--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)
--  Component: Apache Pig (Pig Latin ETL)
--  Owner  : Member 2 — Priya Sharma (Hadoop & MapReduce Specialist)
--  Run    : pig -x mapreduce 03_sentiment_filter.pig
-- ==========================================================================
--  GOAL: isolate positive-sentiment coverage and rank topics by mean sentiment.

-- 1) LOAD the staged enriched corpus.
enriched = LOAD 'data/hadoop_ecosystem/pig/staging/enriched_articles.csv' USING PigStorage(',') AS (
    article_id:chararray, publication:chararray, topic_label:chararray,
    sentiment_polarity:double, word_count:int);

-- 2) FILTER: keep only clearly positive articles (polarity >= 0.2).
positive = FILTER enriched BY sentiment_polarity >= 0.2;

-- 3) GROUP the positive articles by topic.
topic_groups = GROUP positive BY topic_label;

-- 4) Aggregate sentiment statistics per topic.
topic_sentiment = FOREACH topic_groups GENERATE
    group AS topic_label,
    COUNT(positive) AS positive_articles,
    AVG(positive.sentiment_polarity) AS avg_sentiment,
    MAX(positive.sentiment_polarity) AS max_sentiment,
    MIN(positive.sentiment_polarity) AS min_sentiment;

-- 5) ORDER by mean sentiment, highest first.
ranked = ORDER topic_sentiment BY avg_sentiment DESC;

-- 6) STORE + DUMP.
STORE ranked INTO 'data/hadoop_ecosystem/pig/output/03_sentiment_by_topic' USING PigStorage(',');
DUMP ranked;

