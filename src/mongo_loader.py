"""
MongoDB NoSQL Ingestion & Document Store Integration
====================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Authors: Big Data Engineering Team (Member 5: NoSQL & MongoDB Database Specialist)

Description:
------------
Integrates MongoDB document-oriented database with the Big Data pipeline.
Supports both:
1. Live MongoDB instance connection (standard port 27017 or connection URI)
2. Safe offline JSON/BSON simulation mode if live MongoDB daemon is inactive

Features:
---------
- Document schema mapping from PySpark / Parquet records into polymorphic BSON documents
- Dynamic creation of collections:
    * `articles`: Full-text news documents, topic probabilities, and sentiment metrics
    * `topic_definitions`: Discovered LDA topics, term rankings, and Dirichlet weights
    * `publisher_analytics`: Aggregated rollups for real-time operational querying
- Creation of optimal compound and text indices:
    * Compound Index: `{"dominant_topic": 1, "date": -1}` for time-series filtering
    * Text Index: `{"title": "text", "topic_label": "text"}` for semantic retrieval
- MongoDB Aggregation Pipeline execution demonstration ($match, $group, $sort, $project)
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone

# Import PyMongo
try:
    from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False


def _env_int(name, default):
    """Reads a positive integer setting from the environment with a safe default."""
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        print(f"[!] Invalid {name} value; using documented default {default}.")
        return default


# Documented defaults for the two silent sampling caps. Both are configurable
# via CLI flags (--article-limit / --offline-limit) or environment variables.
DEFAULT_ARTICLE_LIMIT = _env_int("MONGO_ARTICLE_LIMIT", 5000)
DEFAULT_OFFLINE_LIMIT = _env_int("MONGO_OFFLINE_LIMIT", 500)


class NewsMongoManager:
    """
    Manages NoSQL ingestion, indexing, and aggregation pipelines in MongoDB.
    """
    def __init__(self, uri="mongodb://localhost:27017/", db_name="news_bigdata_analytics",
                 upsert=False):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self.is_connected = False
        # False (default) keeps the original clean-slate drop() behaviour.
        self.upsert = bool(upsert)
        self._connect()

    def _connect(self):
        """Attempts connection to active MongoDB server with 2-second timeout."""
        if not PYMONGO_AVAILABLE:
            print("[!] PyMongo library is not installed.")
            return

        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=2000)
            # Verify live connectivity
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            self.is_connected = True
            print(f"[+] Successfully connected to live MongoDB instance at: {self.uri}")
            print(f"[+] Active Database: {self.db_name}")
        except (ConnectionFailure, ServerSelectionTimeoutError) as err:
            print(f"[-] Live MongoDB instance not reachable ({err}).")
            print("[*] Switching to Offline Simulation / BSON Export Mode for offline grading & validation.")
            self.is_connected = False

    def ingest_pipeline_results(
        self,
        summary_json_path="data/pipeline_analytics_summary.json",
        parquet_dir="data/processed_parquet",
        offline_export_dir="data/mongodb_offline_collections",
        article_limit=None,
        offline_limit=None
    ):
        """
        Ingests PySpark analytical outputs and Parquet records into MongoDB collections.

        article_limit / offline_limit default to MONGO_ARTICLE_LIMIT (5000) and
        MONGO_OFFLINE_LIMIT (500); a hit on either cap is logged explicitly.
        """
        article_limit = DEFAULT_ARTICLE_LIMIT if article_limit is None else int(article_limit)
        offline_limit = DEFAULT_OFFLINE_LIMIT if offline_limit is None else int(offline_limit)
        print("\n[*] Starting MongoDB Ingestion Pipeline...")
        print(f"[*] Caps: live articles={article_limit:,}, offline sample={offline_limit:,}, "
              f"mode={'upsert' if self.upsert else 'drop/reload'}")

        # Load analytics summary payload
        if not os.path.exists(summary_json_path):
            print(f"[!] Analytics summary not found at {summary_json_path}. Please run spark_pipeline.py first.")
            return False

        with open(summary_json_path, "r", encoding="utf-8") as f:
            summary_data = json.load(f)

        # 1. Prepare Topic Definitions
        topic_docs = []
        topic_labels = summary_data.get("metadata", {}).get("topic_labels", {})
        for topic_name, topic_info in summary_data.get("topics", {}).items():
            doc = {
                "topic_key": topic_name,
                "topic_id": topic_info["topic_id"],
                "terms": topic_info["terms"],
                "weights": topic_info["weights"],
                "label": topic_labels.get(str(topic_info["topic_id"]), topic_info["label_summary"]),
                "label_summary": topic_info["label_summary"],
                "created_at": datetime.now(timezone.utc)
            }
            topic_docs.append(doc)

        # 2. Prepare Publisher Analytics
        publisher_docs = summary_data.get("publisher_distribution", [])

        # 3. Prepare Articles from Parquet
        articles_docs = []
        if os.path.exists(parquet_dir):
            try:
                df_parquet = pd.read_parquet(parquet_dir)
                # Cap defaults to 5,000 documents for a responsive document store;
                # override with --article-limit or MONGO_ARTICLE_LIMIT.
                sample_df = df_parquet.head(article_limit)
                if len(df_parquet) > article_limit:
                    print(f"[!] Article cap HIT: ingesting {article_limit:,} of "
                          f"{len(df_parquet):,} Parquet rows (raise it with "
                          "--article-limit / MONGO_ARTICLE_LIMIT).")
                articles_docs = sample_df.to_dict(orient="records")
                print(f"[+] Loaded {len(articles_docs):,} article documents from Parquet dataset.")
            except Exception as e:
                print(f"[!] Error loading Parquet: {e}")

        # If live MongoDB is connected, write directly
        if self.is_connected:
            self._write_live_collections(topic_docs, publisher_docs, articles_docs)
            self._create_indices()
            self._run_aggregation_queries()
        else:
            # Export offline BSON / JSON document representations
            self._write_offline_mode(topic_docs, publisher_docs, articles_docs,
                                     offline_export_dir, offline_limit)

        return True

    def _write_live_collections(self, topic_docs, publisher_docs, articles_docs):
        """Inserts (or upserts with --upsert) records into live MongoDB collections."""
        # Default remains a clean slate (drop + insert) for deterministic demos.
        # With --upsert, existing documents keyed by topic_key / (publication,
        # topic_label) / article_id are refreshed and new ones are inserted.
        collections = [
            ("topic_definitions", topic_docs, ("topic_key",)),
            ("publisher_analytics", publisher_docs, ("publication", "topic_label")),
            ("articles", articles_docs, ("article_id",)),
        ]
        for name, docs, key_fields in collections:
            if self.upsert:
                self._upsert_docs(name, docs, key_fields)
                continue
            self.db[name].drop()
            if docs:
                self.db[name].insert_many(docs)
                print(f"[+] Inserted {len(docs)} documents into '{name}'.")

    def _upsert_docs(self, collection_name, docs, key_fields):
        """Refreshes documents by natural key without dropping the collection."""
        collection = self.db[collection_name]
        for doc in docs:
            selector = {field: doc.get(field) for field in key_fields}
            collection.update_one(selector, {"$set": doc}, upsert=True)
        print(f"[+] Upserted {len(docs)} documents into '{collection_name}' "
              f"(key: {', '.join(key_fields)}).")

    def _create_indices(self):
        """Defines indexes for high-throughput queries."""
        print("[*] Creating MongoDB compound and text indexes...")
        articles_col = self.db["articles"]
        
        # Compound Index for efficient temporal filtering by topic. Dates are
        # stored as ISO-8601 strings by the Parquet/JSON export, so descending
        # string order equals chronological order; parsing to BSON datetimes
        # would change the shipped exports and is intentionally not done here.
        articles_col.create_index([("dominant_topic", ASCENDING), ("date", DESCENDING)])
        print("    - Created compound index: (dominant_topic ASC, date DESC)")

        # Text Index for keyword search across titles and labels
        articles_col.create_index([("title", TEXT), ("topic_label", TEXT)])
        print("    - Created text search index on title and topic_label")

    def _run_aggregation_queries(self):
        """Demonstrates MongoDB aggregation pipeline ($match, $group, $sort)."""
        print("\n[*] Executing sample MongoDB Aggregation Pipeline ($group by topic_label):")
        # Neutral (0.0) sentiment rows are intentionally excluded so the demo
        # averages reflect opinionated articles only. This is a documented
        # design choice: keep the behaviour for comparability with reports.
        pipeline = [
            {"$match": {"sentiment_polarity": {"$ne": 0.0}}},
            {"$group": {
                "_id": "$topic_label",
                "article_count": {"$sum": 1},
                "avg_sentiment": {"$avg": "$sentiment_polarity"},
                "avg_words": {"$avg": "$clean_word_count"}
            }},
            {"$sort": {"article_count": -1}}
        ]
        results = list(self.db["articles"].aggregate(pipeline))
        for res in results:
            print(f"    Topic: {res['_id']:<35} | Count: {res['article_count']:<5} | Avg Polarity: {res['avg_sentiment']:+.3f}")

    def _write_offline_mode(self, topic_docs, publisher_docs, articles_docs, export_dir,
                            offline_limit=DEFAULT_OFFLINE_LIMIT):
        """Saves collections as structured JSON files for grading without requiring MongoDB daemon."""
        os.makedirs(export_dir, exist_ok=True)
        
        # Serialize topic definitions
        with open(os.path.join(export_dir, "topic_definitions.json"), "w", encoding="utf-8") as f:
            json.dump(topic_docs, f, indent=2, default=str)

        # Serialize publisher analytics
        with open(os.path.join(export_dir, "publisher_analytics.json"), "w", encoding="utf-8") as f:
            json.dump(publisher_docs, f, indent=2, default=str)

        # Offline JSON sample cap (default 500) is now explicit and configurable
        # with --offline-limit / MONGO_OFFLINE_LIMIT.
        sample = articles_docs[:offline_limit]
        if len(articles_docs) > offline_limit:
            print(f"[!] Offline sample cap HIT: exporting {offline_limit:,} of "
                  f"{len(articles_docs):,} articles (raise it with --offline-limit "
                  "/ MONGO_OFFLINE_LIMIT).")

        # Serialize sample articles
        with open(os.path.join(export_dir, "articles_sample.json"), "w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, default=str)

        print(f"[+] Successfully exported offline MongoDB collections to: {export_dir}/")
        print(f"    * topic_definitions.json ({len(topic_docs)} docs)")
        print(f"    * publisher_analytics.json ({len(publisher_docs)} docs)")
        print(f"    * articles_sample.json ({len(sample)} docs)")

if __name__ == "__main__":
    import argparse

    # Honour container/orchestrator overrides (e.g. docker-compose sets MONGO_URI
    # to the 'mongodb' service hostname); fall back to localhost for local runs.
    parser = argparse.ArgumentParser(description="Load pipeline outputs into MongoDB (or offline JSON).")
    parser.add_argument("--uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017/"),
                        help="MongoDB URI (env: MONGO_URI).")
    parser.add_argument("--db-name", default=os.environ.get("MONGO_DB_NAME", "news_bigdata_analytics"),
                        help="Database name (env: MONGO_DB_NAME).")
    parser.add_argument("--article-limit", type=int, default=DEFAULT_ARTICLE_LIMIT,
                        help="Max Parquet articles ingested (env: MONGO_ARTICLE_LIMIT, default 5000).")
    parser.add_argument("--offline-limit", type=int, default=DEFAULT_OFFLINE_LIMIT,
                        help="Max articles in offline JSON sample (env: MONGO_OFFLINE_LIMIT, default 500).")
    parser.add_argument("--upsert", action="store_true",
                        help="Refresh documents by key instead of the default drop/reload.")
    args = parser.parse_args()

    manager = NewsMongoManager(uri=args.uri, db_name=args.db_name, upsert=args.upsert)
    manager.ingest_pipeline_results(article_limit=args.article_limit,
                                    offline_limit=args.offline_limit)
