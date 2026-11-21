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
    import pymongo
    from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False

class NewsMongoManager:
    """
    Manages NoSQL ingestion, indexing, and aggregation pipelines in MongoDB.
    """
    def __init__(self, uri="mongodb://localhost:27017/", db_name="news_bigdata_analytics"):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self.is_connected = False
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
        offline_export_dir="data/mongodb_offline_collections"
    ):
        """
        Ingests PySpark analytical outputs and Parquet records into MongoDB collections.
        """
        print("\n[*] Starting MongoDB Ingestion Pipeline...")

        # Load analytics summary payload
        if not os.path.exists(summary_json_path):
            print(f"[!] Analytics summary not found at {summary_json_path}. Please run spark_pipeline.py first.")
            return False

        with open(summary_json_path, "r", encoding="utf-8") as f:
            summary_data = json.load(f)

        # 1. Prepare Topic Definitions
        topic_docs = []
        for topic_name, topic_info in summary_data.get("topics", {}).items():
            doc = {
                "topic_key": topic_name,
                "topic_id": topic_info["topic_id"],
                "terms": topic_info["terms"],
                "weights": topic_info["weights"],
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
                # Sample up to 5,000 documents for responsive document store ingestion
                sample_df = df_parquet.head(5000)
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
            self._write_offline_mode(topic_docs, publisher_docs, articles_docs, offline_export_dir)

        return True

    def _write_live_collections(self, topic_docs, publisher_docs, articles_docs):
        """Inserts records into live MongoDB collections."""
        # Clean slate or upsert
        self.db["topic_definitions"].drop()
        if topic_docs:
            self.db["topic_definitions"].insert_many(topic_docs)
            print(f"[+] Inserted {len(topic_docs)} documents into 'topic_definitions'.")

        self.db["publisher_analytics"].drop()
        if publisher_docs:
            self.db["publisher_analytics"].insert_many(publisher_docs)
            print(f"[+] Inserted {len(publisher_docs)} documents into 'publisher_analytics'.")

        self.db["articles"].drop()
        if articles_docs:
            self.db["articles"].insert_many(articles_docs)
            print(f"[+] Inserted {len(articles_docs)} documents into 'articles'.")

    def _create_indices(self):
        """Defines indexes for high-throughput queries."""
        print("[*] Creating MongoDB compound and text indexes...")
        articles_col = self.db["articles"]
        
        # Compound Index for efficient temporal filtering by topic
        articles_col.create_index([("dominant_topic", ASCENDING), ("date", DESCENDING)])
        print("    - Created compound index: (dominant_topic ASC, date DESC)")

        # Text Index for keyword search across titles and labels
        articles_col.create_index([("title", TEXT), ("topic_label", TEXT)])
        print("    - Created text search index on title and topic_label")

    def _run_aggregation_queries(self):
        """Demonstrates MongoDB aggregation pipeline ($match, $group, $sort)."""
        print("\n[*] Executing sample MongoDB Aggregation Pipeline ($group by topic_label):")
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

    def _write_offline_mode(self, topic_docs, publisher_docs, articles_docs, export_dir):
        """Saves collections as structured JSON files for grading without requiring MongoDB daemon."""
        os.makedirs(export_dir, exist_ok=True)
        
        # Serialize topic definitions
        with open(os.path.join(export_dir, "topic_definitions.json"), "w", encoding="utf-8") as f:
            json.dump(topic_docs, f, indent=2, default=str)

        # Serialize publisher analytics
        with open(os.path.join(export_dir, "publisher_analytics.json"), "w", encoding="utf-8") as f:
            json.dump(publisher_docs, f, indent=2, default=str)

        # Serialize sample articles
        with open(os.path.join(export_dir, "articles_sample.json"), "w", encoding="utf-8") as f:
            json.dump(articles_docs[:500], f, indent=2, default=str)

        print(f"[+] Successfully exported offline MongoDB collections to: {export_dir}/")
        print(f"    * topic_definitions.json ({len(topic_docs)} docs)")
        print(f"    * publisher_analytics.json ({len(publisher_docs)} docs)")
        print(f"    * articles_sample.json ({len(articles_docs[:500])} docs)")

if __name__ == "__main__":
    # Honour container/orchestrator overrides (e.g. docker-compose sets MONGO_URI
    # to the 'mongodb' service hostname); fall back to localhost for local runs.
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.environ.get("MONGO_DB_NAME", "news_bigdata_analytics")
    manager = NewsMongoManager(uri=uri, db_name=db_name)
    manager.ingest_pipeline_results()
