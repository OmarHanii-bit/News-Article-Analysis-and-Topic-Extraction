-- ==========================================================================
--  01_clean_and_count.pig
--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)
--  Component: Apache Pig (Pig Latin ETL)
--  Owner  : Member 2 — Priya Sharma (Hadoop & MapReduce Specialist)
--  Run    : pig -x mapreduce 01_clean_and_count.pig
-- ==========================================================================
--  GOAL: load the raw news CSV, clean + tokenize the article body, count
--        global word frequencies and store the top 100 terms.

-- 1) LOAD the raw CSV with the built-in PigStorage(',') loader.
articles = LOAD 'data/raw_news_articles.csv' USING PigStorage(',') AS (
    article_id:chararray, title:chararray, author:chararray, date:chararray,
    year:int, month:int, publication:chararray, category:chararray,
    sub_topic:chararray, content:chararray, word_count:int, url:chararray);

-- 2) FILTER out the CSV header row and any empty bodies.
data = FILTER articles BY article_id != 'article_id' AND word_count > 0;

-- 3) CLEAN: lowercase the body and replace every non-letter run with a space.
cleaned = FOREACH data GENERATE
    article_id,
    REPLACE(LOWER(content), '[^a-z]+', ' ') AS clean_content;

-- 4) TOKENIZE: FLATTEN turns the list of tokens into one row per word.
tokens = FOREACH cleaned GENERATE FLATTEN(TOKENIZE(clean_content)) AS word;

-- 5) GROUP by word and COUNT the occurrences in each bag.
word_groups = GROUP tokens BY word;
word_counts = FOREACH word_groups GENERATE group AS word, COUNT(tokens) AS cnt;

-- 6) ORDER by descending frequency and keep the top 100 terms.
ranked = ORDER word_counts BY cnt DESC;
top_terms = LIMIT ranked 100;

-- 7) STORE the result as a comma-separated part file.
STORE top_terms INTO 'data/hadoop_ecosystem/pig/output/01_word_counts' USING PigStorage(',');

-- 8) DUMP for a quick console sanity check.
DUMP top_terms;

