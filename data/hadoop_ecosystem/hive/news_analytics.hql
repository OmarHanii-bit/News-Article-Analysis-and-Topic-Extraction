-- ==========================================================================
--  news_analytics.hql
--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)
--  Component: Apache Hive data warehouse
--  Owner  : Member 3 — Yasmine Mohamed (Distributed Warehouse Engineer)
--  Run    : hive -f news_analytics.hql
-- ==========================================================================

-- ---- Warehouse / dynamic-partition tuning -------------------------------
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.cbo.enable=true;
SET hive.auto.convert.join=true;
SET hive.stats.autogather=true;

-- ---- EXTERNAL table over the Spark-written Parquet lakehouse ------------
--      EXTERNAL => dropping the table does NOT delete the HDFS data.
--      PARTITIONED BY (year, month) mirrors the `year=.../month=...`
--      directory layout, enabling partition pruning.
CREATE EXTERNAL TABLE IF NOT EXISTS news_articles (
    article_id         STRING,
    title              STRING,
    author             STRING,
    date               STRING,
    publication        STRING,
    category           STRING,
    sub_topic          STRING,
    clean_word_count   INT,
    dominant_topic     INT,
    topic_label        STRING,
    topic_confidence   DOUBLE,
    sentiment_polarity DOUBLE
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '/user/news/lakehouse'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Load partition metadata for every existing partition directory.
MSCK REPAIR TABLE news_articles;

-- ---- (a) Topic volume ranking -------------------------------------------
SELECT topic_label,
       COUNT(*)                          AS article_count,
       ROUND(AVG(sentiment_polarity), 4) AS mean_sentiment
FROM news_articles
GROUP BY topic_label
ORDER BY article_count DESC;

-- ---- (b) Publisher x topic cross-tabulation (conditional aggregation) ---
SELECT publication,
        SUM(CASE WHEN topic_label = 'AI Infrastructure & Compute' THEN 1 ELSE 0 END) AS `AI_Infrastructure_Compute`,
        SUM(CASE WHEN topic_label = 'Clinical Medicine & Genomics' THEN 1 ELSE 0 END) AS `Clinical_Medicine_Genomics`,
        SUM(CASE WHEN topic_label = 'Grid Decarbonization & Clean Energy' THEN 1 ELSE 0 END) AS `Grid_Decarbonization_Clean_Energy`,
        SUM(CASE WHEN topic_label = 'Markets, Politics & Regulation' THEN 1 ELSE 0 END) AS `Markets_Politics_Regulation`,
        SUM(CASE WHEN topic_label = 'Quantum Physics & Advanced Materials' THEN 1 ELSE 0 END) AS `Quantum_Physics_Advanced_Materials`
FROM news_articles
GROUP BY publication
ORDER BY SUM(1) DESC
LIMIT 10;

-- ---- (c) Month-over-month article volume with LAG window -----------------
WITH monthly AS (
    SELECT topic_label, year, month, COUNT(*) AS cnt
    FROM news_articles
    GROUP BY topic_label, year, month
)
SELECT topic_label, year, month, cnt,
       cnt - LAG(cnt) OVER (PARTITION BY topic_label ORDER BY year, month) AS mom_delta
FROM monthly
ORDER BY topic_label, year, month;

-- ---- (d) Sentiment ranking of publishers within each topic (RANK) -------
WITH pub_topic AS (
    SELECT topic_label, publication, AVG(sentiment_polarity) AS avg_sent
    FROM news_articles
    GROUP BY topic_label, publication
)
SELECT topic_label, publication,
       ROUND(avg_sent, 4) AS avg_sent,
       RANK() OVER (PARTITION BY topic_label ORDER BY avg_sent DESC) AS rnk
FROM pub_topic
ORDER BY topic_label, rnk
LIMIT 30;

-- ---- (e) Topic keyword importance: JOIN topics with MapReduce term counts -
--      `topics` holds one (topic_label, term) row per LDA keyword.
SELECT t.topic_label,
       COUNT(DISTINCT tc.term) AS matched_terms,
       SUM(tc.cnt)             AS total_keyword_freq
FROM topics t
JOIN term_counts tc ON (t.term = tc.term)
GROUP BY t.topic_label
ORDER BY total_keyword_freq DESC;

-- ---- (f) Partition-pruned single-month query ----------------------------
--      Resolved partition: year=2025/month=12
SELECT COUNT(*)                       AS article_count,
       ROUND(AVG(sentiment_polarity),4) AS mean_sentiment
FROM news_articles
WHERE year = 2025 AND month = 12;

-- ---- Keep warehouse statistics fresh for the cost-based optimiser -------
ANALYZE TABLE news_articles COMPUTE STATISTICS;
ANALYZE TABLE news_articles COMPUTE STATISTICS FOR COLUMNS topic_label, publication;

