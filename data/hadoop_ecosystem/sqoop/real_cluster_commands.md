# Apache Sqoop — Real Production Cluster Commands

Project : News Article Analysis and Topic Extraction
Owner   : Member 5 — Mohamed Ibrahim (NoSQL & Data Integration Specialist)

These are the genuine Sqoop command lines used to perform the very same
RDBMS <-> HDFS/Hive transfers that this module simulates locally.
Replace host/credentials with your environment's values.

## 0. Environment

```bash
export SQOOP_HOME=/opt/cloudera/parcels/SQOOP
export HADOOP_CONF_DIR=/etc/hadoop/conf
export JDBC_URL="jdbc:mysql://rdbms-host:3306/news"
export JDBC_USER="etl"
export JDBC_DRIVER="com.mysql.cj.jdbc.Driver"
JDBC_PASSWORD_FILE=/etc/sqoop/conf/etl.password
```

## 1. List the RDBMS tables Sqoop can see

```bash
sqoop list-tables --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE"
```

## 2. Import the `publishers` dimension (RDBMS -> HDFS, 4 mappers, Parquet)

```bash
sqoop import \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --table publishers \
  --target-dir /user/news/sqoop_import/publishers \
  --m 4 --as-parquetfile --delete-target-dir
```

## 3. Import `publishers` straight into Hive

```bash
sqoop import \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --table publishers \
  --hive-import --hive-table news_db.publishers --create-hive-table --m 2
```

## 4. Import `articles_staging` with column selection + split-by

```bash
sqoop import \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --table articles_staging \
  --columns "article_id,title,publication,category,word_count,sentiment_polarity" \
  --where "word_count > 0" \
  --split-by publisher_id \
  --target-dir /user/news/sqoop_import/articles_staging \
  --m 4
```

## 5. Free-form query import (a join across the staging tables)

```bash
sqoop import \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --query "SELECT a.article_id, p.publication, p.country, a.category, a.word_count, a.sentiment_polarity \
           FROM articles_staging a JOIN publishers p ON a.publication = p.publication \
           WHERE \$CONDITIONS" \
  --split-by a.publication \
  --target-dir /user/news/sqoop_import/articles_enriched --m 4
```

## 6. Incremental append import (only rows newer than the watermark)

```bash
sqoop import \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --table articles_staging --incremental append \
  --check-column article_id --last-value "ART_105500" \
  --target-dir /user/news/sqoop_import/articles_incremental --m 4
```

## 7. Export the enriched analytics result back into the RDBMS (UPSERT)

```bash
sqoop export \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --table articles_exported \
  --export-dir /user/news/export \
  --update-key article_id --update-mode allowinsert \
  --input-fields-terminated-by ',' --m 4
```

## 8. Saved Sqoop job (persist the incremental job on the metastore)

```bash
sqoop job --create news_incremental_import -- import \
  --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --table articles_staging --incremental append \
  --check-column article_id --last-value "0" \
  --target-dir /user/news/sqoop_import/articles_stream --m 4

sqoop job --exec news_incremental_import
sqoop job --list
sqoop job --show news_incremental_import
```

## 9. Ad-hoc SQL evaluation against the RDBMS (smoke test)

```bash
sqoop eval --connect "$JDBC_URL" --username "$JDBC_USER" --password-file "$JDBC_PASSWORD_FILE" \
  --query "SELECT COUNT(*) AS n FROM articles_staging"
```

## Local simulation result on this workstation
- RDBMS tables  : publishers, articles_staging, articles_exported
- Rows imported : 6008
- Rows exported : 6000
