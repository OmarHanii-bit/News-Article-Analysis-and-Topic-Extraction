"""
Distributed News Article Processing & Topic Extraction Pipeline (PySpark)
========================================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Authors: 7-Member Big Data Engineering & Analytics Team
         - Member 1: Project Lead & Architecture Specialist
         - Member 3: PySpark Distributed Computing Engineer
         - Member 4: NLP & Machine Learning Scientist

Detailed Architecture & Implementation:
--------------------------------------
1. SparkSession Initialization: Configures local distributed cluster with memory optimization.
2. Schema Enforcement: Uses explicit StructType to prevent schema drift across messy sources.
3. Text Hygiene & Cleansing: Utilizes Spark SQL regex replacements to purge HTML, URLs, and artifacts.
4. Distributed Feature Engineering:
   - RegexTokenizer: Tokenizes text into word sequences.
   - StopWordsRemover: Discards non-informative lexical tokens.
   - CountVectorizer: Computes distributed term frequencies across vocabulary.
   - IDF (Inverse Document Frequency): Computes statistical term significance.
5. Distributed Unsupervised ML (LDA):
   - Trains Latent Dirichlet Allocation model with k=6 latent topics.
   - Extracts top vocabulary terms per topic with Dirichlet weight coefficients.
   - Assigns dominant topic IDs and probability vectors to each article document.
6. Lexical Sentiment & Metric Profiling:
   - UDF for sentiment polarity scoring.
7. Analytical Aggregations & Storage:
   - Topic distribution across global publishers.
   - Temporal trend evolution over months and years.
   - Partitioned Parquet export (simulating distributed HDFS sink).
   - Export structured JSON for MongoDB document store and frontend visualization.
"""

import os
import json
import time
import argparse

# PySpark Core and SQL Modules
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, when, lower, trim, regexp_replace, size

# PySpark Machine Learning Modules
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
)
from pyspark.ml.clustering import LDA

# NLTK / Lexicon helper for distributed sentiment scoring
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Ensure required NLTK lexical resources are available
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

# ---------------------------------------------------------------------------
# Topic labelling strategy
# ---------------------------------------------------------------------------
# Legacy fallback map only. LDA topic indices are NOT stable across runs, so
# production paths call derive_cluster_labels() instead of trusting this dict.
# These values match the current 2026-11-27 run for backwards compatibility.
CANONICAL_TOPIC_LABELS = {
    0: "Clinical Medicine & Genomics",
    1: "Grid Decarbonization & Clean Energy",
    2: "Quantum Physics & Advanced Materials",
    3: "Markets, Politics & Regulation",
    4: "Unused topic (no dominant assignments)",
    5: "AI Infrastructure & Compute"
}

# Post-hoc naming map for the synthetic benchmark. LDA topic indices are
# NOT stable across runs, so labels must follow each cluster's actual content
# rather than a fixed index. The synthetic generator exposes a `category`
# column; we use its majority value only to NAME a cluster, never to train or
# reassign documents. The clustering itself remains fully unsupervised.
CATEGORY_TO_TOPIC_LABEL = {
    "Technology": "AI Infrastructure & Compute",
    "Business": "Monetary Policy & Markets",
    "Politics": "Antitrust & Regulatory Policy",
    "Healthcare": "Clinical Medicine & Genomics",
    "Climate": "Grid Decarbonization & Clean Energy",
    "Science": "Quantum Physics & Advanced Materials",
}

CATEGORY_SHORT_LABEL = {
    "Technology": "AI",
    "Business": "Markets",
    "Politics": "Politics",
    "Healthcare": "Clinical",
    "Climate": "Clean Energy",
    "Science": "Quantum",
}


def derive_cluster_labels(df_with_topics, num_topics, extracted_topics):
    """
    Names each LDA cluster from its majority ``category``.

    Returns ``(labels_by_topic, diagnostics)``. A cluster that mixes Business
    and Politics is named explicitly because k=6 on this corpus merges those
    two domains; an empty topic is marked as unused. If the corpus has no
    usable category column, the function falls back to term-derived labels.
    """
    rows = (
        df_with_topics
        .groupBy("dominant_topic", "category")
        .count()
        .collect()
    )
    counts = {}
    for row in rows:
        tid = int(row["dominant_topic"])
        category = row["category"] or "Unknown"
        counts.setdefault(tid, {})[str(category)] = int(row["count"])

    labels = {}
    diagnostics = {}
    for tid in range(num_topics):
        topic_counts = counts.get(tid, {})
        total = sum(topic_counts.values())
        if total == 0:
            labels[tid] = "Unused topic (no dominant assignments)"
            diagnostics[tid] = {"total": 0, "majority": None, "share": 0.0}
            continue

        ranked = sorted(topic_counts.items(), key=lambda item: (-item[1], item[0]))
        top_category, top_count = ranked[0]
        top_share = top_count / total
        diagnostics[tid] = {
            "total": total,
            "majority": top_category,
            "share": round(top_share, 4),
        }

        if top_category in CATEGORY_TO_TOPIC_LABEL and top_share >= 0.75:
            labels[tid] = CATEGORY_TO_TOPIC_LABEL[top_category]
        elif (
            top_category in CATEGORY_TO_TOPIC_LABEL
            and len(ranked) > 1
            and {top_category, ranked[1][0]} == {"Business", "Politics"}
        ):
            # Honest name for the merged markets/regulatory cluster.
            labels[tid] = "Markets, Politics & Regulation"
        elif top_category in CATEGORY_TO_TOPIC_LABEL:
            second = ranked[1][0]
            labels[tid] = (
                f"{CATEGORY_TO_TOPIC_LABEL[top_category]} & "
                f"{CATEGORY_SHORT_LABEL.get(second, second)}"
            )
        else:
            terms = extracted_topics.get(f"Topic_{tid}", {}).get("terms", [])
            labels[tid] = derive_topic_label(terms)

    return labels, diagnostics

# High-frequency news filler words that make poor topic labels when derived
# automatically from the LDA top-term lists of a real-world corpus.
LABEL_STOPWORDS = {
    "would", "could", "should", "also", "said", "says", "say", "one", "two",
    "three", "new", "first", "last", "year", "years", "time", "times", "day",
    "days", "week", "month", "percent", "million", "billion", "trillion",
    "including", "according", "report", "reports", "reported", "raises",
    "raise", "raised", "rose", "rise", "fell", "fall", "amid", "via", "us",
    "its", "their", "his", "her", "they", "them", "been", "being", "will",
    "may", "might", "must", "can", "get", "got", "make", "made", "many",
    "much", "more", "most", "than", "then", "over", "under", "about",
    "after", "before", "during", "between", "within", "without", "against",
    "still", "even", "back", "around", "since", "until", "while", "where",
}


def derive_topic_label(top_words, max_words: int = 3) -> str:
    """
    Builds a human-readable topic label straight from the model's top terms.

    This keeps the pipeline correct on ANY corpus (including real Reuters
    wire stories) where the synthetic canonical labels would be meaningless.
    """
    picked = [w for w in top_words if w.lower() not in LABEL_STOPWORDS][:max_words]
    if not picked:
        picked = [w for w in top_words[:max_words]]
    if not picked:
        return "Unlabeled Topic"
    return " / ".join(w.title() for w in picked)


# Initialize single VADER instance for worker serialization
_vader_analyzer = None

def get_vader():
    global _vader_analyzer
    if _vader_analyzer is None:
        _vader_analyzer = SentimentIntensityAnalyzer()
    return _vader_analyzer

def compute_vader_sentiment(text: str) -> float:
    """
    Computes compound sentiment polarity score (-1.0 to +1.0) using VADER lexicon.
    Executed in parallel across distributed Spark executor worker partitions.
    Note: scoring is intentionally bounded to the first 1,000 characters of the
    cleaned article body to control distributed UDF cost (see implementation).
    """
    if not text or not isinstance(text, str):
        return 0.0
    analyzer = get_vader()
    # Bounded-cost design choice: only the first 1,000 characters are scored so
    # the UDF stays cheap inside every Spark executor partition. Long article
    # tails are deliberately ignored; this is unchanged for comparability with
    # the shipped analytics and visualizations.
    scores = analyzer.polarity_scores(text[:1000])
    return float(scores.get("compound", 0.0))

def extract_dominant_topic(topic_distribution):
    """
    Identifies the index of the highest probability topic in the DenseVector.
    """
    if topic_distribution is None:
        return -1
    values = list(topic_distribution)
    max_val = max(values)
    return int(values.index(max_val))

def extract_dominant_topic_prob(topic_distribution):
    """
    Returns the maximum probability score corresponding to the dominant topic.
    """
    if topic_distribution is None:
        return 0.0
    return float(max(list(topic_distribution)))

def init_spark_session(app_name: str = "NewsArticleTopicExtraction") -> SparkSession:
    """
    Initializes an enterprise-configured SparkSession with optimized memory parameters.
    """
    print("[*] Initializing PySpark Session...")
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Orchestrator/CI can override these without editing source code.
        .master(os.environ.get("SPARK_MASTER", "local[*]"))  # all cores by default
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", "8") # Balanced partitions for local multi-threading
        .config("spark.default.parallelism", "8")
        .config("spark.ui.enabled", "false") # Suppress UI overhead during batch pipeline runs
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN") # Suppress verbose JVM log spam
    print(f"[+] PySpark initialized. Spark Version: {spark.version}")
    return spark

def define_explicit_schema() -> StructType:
    """
    Defines an explicit schema to enforce strict data contracts upon CSV ingestion.
    """
    return StructType([
        StructField("article_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("publication", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_topic", StringType(), True),
        StructField("content", StringType(), True),
        StructField("word_count", IntegerType(), True),
        StructField("url", StringType(), True),
    ])

def run_distributed_pipeline(
    input_csv_path: str = "data/raw_news_articles.csv",
    output_parquet_dir: str = "data/processed_parquet",
    output_summary_json: str = "data/pipeline_analytics_summary.json",
    num_topics: int = 6,
    max_iterations: int = 20,
    vocab_size: int = 5000,
    auto_labels: bool = False,
    topic_labels_map: dict = None
):
    """
    Orchestrates the complete distributed processing and topic extraction pipeline.

    The pipeline is corpus-agnostic: it can process the canonical synthetic demo
    dataset (default) OR a real full-text corpus (e.g. Reuters wire stories)
    simply by pointing ``input_csv_path`` at a different file. Pass
    ``auto_labels=True`` for real corpora so topic labels are derived from the
    learned top terms instead of the majority-category synthetic labels.
    """
    start_time = time.time()
    spark = init_spark_session()

    # -------------------------------------------------------------------------
    # STAGE 1: INGESTION & STRICT SCHEMA ENFORCEMENT
    # -------------------------------------------------------------------------
    print(f"\n[Stage 1/7] Ingesting raw dataset with schema enforcement: {input_csv_path}")
    raw_schema = define_explicit_schema()
    
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("escape", "\"")
        .option("multiline", "true")
        .schema(raw_schema)
        .load(input_csv_path)
    )
    
    total_raw_count = df_raw.count()
    print(f"[+] Raw ingested document count: {total_raw_count:,}")

    # -------------------------------------------------------------------------
    # STAGE 2: DATA CLEANING, IMPUTATION & TEXT HYGIENE
    # -------------------------------------------------------------------------
    print("\n[Stage 2/7] Executing distributed data cleaning and text normalization...")
    
    # 2.1 Filter out records with null article_id or content
    df_filtered = df_raw.filter(
        col("article_id").isNotNull() & 
        col("content").isNotNull() & 
        (trim(col("content")) != "")
    )
    
    # 2.2 Impute missing authors
    df_imputed = df_filtered.withColumn(
        "author",
        when(col("author").isNull() | (trim(col("author")) == ""), "Staff Correspondent")
        .otherwise(col("author"))
    )

    # 2.3 Strip HTML markup, web links, XML entities, and special characters
    clean_expr = regexp_replace(col("content"), r"<[^>]+>", " ")             # Remove HTML tags
    clean_expr = regexp_replace(clean_expr, r"http[s]?://\S+", " ")           # Remove web URLs
    clean_expr = regexp_replace(clean_expr, r"&[a-z0-9#]+;", " ")             # Remove HTML entities
    clean_expr = regexp_replace(clean_expr, r"[^a-zA-Z\s]", " ")              # Strip non-alphabetic chars
    clean_expr = regexp_replace(clean_expr, r"\s+", " ")                      # Collapse multiple spaces

    df_cleaned = df_imputed.withColumn("clean_content", trim(lower(clean_expr)))
    
    # Calculate cleaned word length
    df_cleaned = df_cleaned.withColumn("clean_word_count", size(F.split(col("clean_content"), " ")))
    
    # Filter out empty or trivially short articles (< 10 words)
    df_valid = df_cleaned.filter(col("clean_word_count") >= 10)
    valid_count = df_valid.count()
    print(f"[+] Valid cleaned documents retained: {valid_count:,} (Purged: {total_raw_count - valid_count})")

    # -------------------------------------------------------------------------
    # STAGE 3: DISTRIBUTED FEATURE ENGINEERING (NLP PIPELINE)
    # -------------------------------------------------------------------------
    print("\n[Stage 3/7] Building distributed NLP pipeline (Tokenizer -> StopWordsRemover -> CountVectorizer -> IDF)...")
    
    # Tokenizer: splits text into tokens
    tokenizer = RegexTokenizer(
        inputCol="clean_content",
        outputCol="raw_tokens",
        pattern=r"\s+",
        minTokenLength=3
    )
    df_tokenized = tokenizer.transform(df_valid)

    # Custom domain stopwords to complement standard PySpark stopwords
    domain_stopwords = [
        "said", "also", "would", "could", "one", "two", "new", "first", "last",
        "year", "years", "time", "day", "days", "reuters", "bloomberg", "associated",
        "press", "times", "journal", "click", "read", "subscribe", "newsletter", "advertisement"
    ]
    all_stopwords = StopWordsRemover.loadDefaultStopWords("english") + domain_stopwords

    # Stopwords Remover
    remover = StopWordsRemover(
        inputCol="raw_tokens",
        outputCol="filtered_tokens",
        stopWords=all_stopwords
    )
    df_filtered_tokens = remover.transform(df_tokenized)

    # CountVectorizer: creates sparse term-frequency count vectors.
    # minDF is data-driven: the normal 3-document floor keeps noisy hapax terms
    # out of a full corpus, but on a tiny/sample corpus (fewer than 3 surviving
    # documents) it would leave an empty vocabulary, so fall back to 1.0.
    min_df = 3.0 if valid_count >= 3 else 1.0
    vectorizer = CountVectorizer(
        inputCol="filtered_tokens",
        outputCol="tf_features",
        vocabSize=vocab_size,
        minDF=min_df
    )
    cv_model = vectorizer.fit(df_filtered_tokens)
    df_tf = cv_model.transform(df_filtered_tokens)
    vocabulary = cv_model.vocabulary
    print(f"[+] Vocabulary built with {len(vocabulary):,} unique terms (minDF={min_df}).")

    # IDF Model: calculates inverse document frequency weights.
    # IDF features are computed for downstream TF-IDF analytics/export; LDA intentionally uses raw count features to preserve comparability with shipped results.
    idf = IDF(inputCol="tf_features", outputCol="tfidf_features")
    idf_model = idf.fit(df_tf)
    df_features = idf_model.transform(df_tf)

    # Cache feature dataframe in executor memory for iterative ML training
    df_features.persist()

    # -------------------------------------------------------------------------
    # STAGE 4: UNSUPERVISED MACHINE LEARNING (LATENT DIRICHLET ALLOCATION)
    # -------------------------------------------------------------------------
    print(f"\n[Stage 4/7] Training Distributed Latent Dirichlet Allocation (LDA) with k={num_topics} topics...")
    lda = LDA(
        k=num_topics,
        maxIter=max_iterations,
        featuresCol="tf_features",
        topicDistributionCol="topicDistribution",
        optimizer="online", # Online variational Bayes is highly scalable for large corpora
        seed=42
    )
    lda_model = lda.fit(df_features)

    # Extract top keywords per topic
    num_keywords = 12
    topics_matrix = lda_model.describeTopics(maxTermsPerTopic=num_keywords)
    topics_collected = topics_matrix.collect()

    extracted_topics = {}
    print("\n--- Extracted LDA Topics & Dominant Terms ---")
    for row in topics_collected:
        topic_idx = row["topic"]
        term_indices = row["termIndices"]
        term_weights = row["termWeights"]
        top_words = [vocabulary[idx] for idx in term_indices]
        extracted_topics[f"Topic_{topic_idx}"] = {
            "topic_id": topic_idx,
            "terms": top_words,
            "weights": [float(round(w, 4)) for w in term_weights],
            "label_summary": ", ".join(top_words[:5])
        }
        print(f"Topic {topic_idx}: {', '.join(top_words[:6])} (Top weights: {term_weights[0]:.3f}, {term_weights[1]:.3f})")

    # Transform documents to append topic probability distributions
    df_with_topics = lda_model.transform(df_features)

    # -------------------------------------------------------------------------
    # STAGE 5: DOMINANT TOPIC INFERENCE & SENTIMENT ENRICHMENT
    # -------------------------------------------------------------------------
    print("\n[Stage 5/7] Inferring dominant topics & scoring sentiment polarity via distributed UDFs...")
    
    # Register Spark UDFs
    udf_dominant_topic = udf(extract_dominant_topic, IntegerType())
    udf_dominant_prob = udf(extract_dominant_topic_prob, FloatType())
    udf_sentiment = udf(compute_vader_sentiment, FloatType())

    df_enriched = (
        df_with_topics
        .withColumn("dominant_topic", udf_dominant_topic(col("topicDistribution")))
        .withColumn("topic_confidence", udf_dominant_prob(col("topicDistribution")))
        .withColumn("sentiment_polarity", udf_sentiment(col("clean_content")))
    )

    # Assign interpretable topic labels.
    #   - Synthetic mode (default): name each cluster from its majority category
    #     (post-hoc naming only; clustering stays unsupervised).
    #   - Auto mode (real corpora) : labels derived from each topic's top terms.
    #   - Explicit topic_labels_map still wins when supplied by a caller.
    label_method = "provided-map" if topic_labels_map else (
        "term-derived" if auto_labels else "majority-category"
    )
    if auto_labels:
        active_labels = {}
        for tid in range(num_topics):
            # describeTopics() can omit a topic on degenerate corpora; .get()
            # keeps auto-labelling total instead of raising a KeyError.
            topic_terms = extracted_topics.get(f"Topic_{tid}", {}).get("terms", [])
            active_labels[tid] = derive_topic_label(topic_terms)
        label_diagnostics = {}
        print("\n[*] Auto-derived topic labels from learned top terms:")
    elif topic_labels_map:
        active_labels = dict(topic_labels_map)
        label_diagnostics = {}
        print("\n[*] Using caller-supplied topic labels:")
    else:
        active_labels, label_diagnostics = derive_cluster_labels(
            df_enriched, num_topics, extracted_topics
        )
        print("\n[*] Semantically aligned topic labels (majority category per cluster):")
    for tid in range(num_topics):
        print(f"    Topic {tid}: {active_labels.get(tid, f'Topic {tid}')}")

    # Create mapping expression
    label_expr = when(col("dominant_topic") == 0, active_labels.get(0, "Topic 0"))
    for tid in range(1, num_topics):
        label_expr = label_expr.when(col("dominant_topic") == tid, active_labels.get(tid, f"Topic {tid}"))
    label_expr = label_expr.otherwise("Unassigned")

    df_final = df_enriched.withColumn("topic_label", label_expr)

    # -------------------------------------------------------------------------
    # STAGE 6: ANALYTICAL QUERIES & MULTI-DIMENSIONAL ROLLUPS
    # -------------------------------------------------------------------------
    print("\n[Stage 6/7] Computing complex analytical rollups across topics, publishers, and time...")

    # Query A: Topic distribution by Publisher
    topic_by_pub = (
        df_final
        .groupBy("publication", "topic_label")
        .count()
        .orderBy("publication", F.desc("count"))
    ).toPandas()

    # Query B: Temporal evolution (Year-Month topic counts)
    temporal_trends = (
        df_final
        .groupBy("year", "month", "topic_label")
        .agg(
            F.count("article_id").alias("article_count"),
            F.avg("sentiment_polarity").alias("avg_sentiment"),
            F.avg("clean_word_count").alias("avg_word_count")
        )
        .orderBy("year", "month", "topic_label")
    ).toPandas()

    # Query C: Topic Summary Statistics
    topic_summary = (
        df_final
        .groupBy("topic_label")
        .agg(
            F.count("article_id").alias("total_articles"),
            F.avg("sentiment_polarity").alias("mean_sentiment"),
            F.avg("topic_confidence").alias("mean_confidence"),
            F.avg("clean_word_count").alias("mean_length")
        )
        .orderBy(F.desc("total_articles"))
    ).toPandas()

    print("\n--- Topic Summary Metrics ---")
    print(topic_summary.to_string(index=False))

    # -------------------------------------------------------------------------
    # STAGE 7: STORAGE & EXPORT (HDFS PARQUET & JSON METADATA)
    # -------------------------------------------------------------------------
    print(f"\n[Stage 7/7] Persisting partitioned Parquet lakehouse storage: {output_parquet_dir}")
    
    # Select columns for downstream analytics lakehouse
    export_cols = [
        "article_id", "title", "author", "date", "year", "month", 
        "publication", "category", "sub_topic", "clean_word_count",
        "dominant_topic", "topic_label", "topic_confidence", "sentiment_polarity"
    ]
    
    df_export = df_final.select(*export_cols)

    # Cross-platform Parquet export: attempt the native Spark writer, then fall
    # back to a bounded PyArrow export. On Windows the usual cause is a missing
    # winutils.exe / Hadoop native library, but the cause MAY BE UNRELATED, so
    # the exception type and message are logged instead of being assumed.
    try:
        from py4j.protocol import Py4JError as _SparkWriterError
    except ImportError:  # pragma: no cover - py4j ships with PySpark
        _SparkWriterError = OSError
    parquet_fallback_max_rows = 50000  # driver-memory guard; > full 6k corpus
    try:
        (
            df_export
            .write
            .mode("overwrite")
            .partitionBy("year", "month")
            .parquet(output_parquet_dir)
        )
        print(f"[+] Native Spark partitioned Parquet dataset saved at {output_parquet_dir}")
    except (OSError, ImportError, RuntimeError, ValueError, _SparkWriterError) as write_err:
        print(f"[*] Native Spark Parquet writer failed "
              f"({type(write_err).__name__}: {write_err}).")
        print("[*] Often this is missing winutils.exe/Hadoop native libs on "
              "Windows, but the cause may be unrelated; using bounded PyArrow fallback.")
        # Bound the driver-side fallback so a very large corpus cannot exhaust
        # local RAM; the default ceiling comfortably covers the shipped corpus.
        df_export_pd = df_export.limit(parquet_fallback_max_rows).toPandas()
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        os.makedirs(output_parquet_dir, exist_ok=True)
        table = pa.Table.from_pandas(df_export_pd)
        pq.write_to_dataset(
            table,
            root_path=output_parquet_dir,
            partition_cols=["year", "month"],
            use_dictionary=True,
            compression="snappy"
        )
        print(f"[+] Partitioned Parquet lakehouse successfully written via PyArrow at "
              f"{output_parquet_dir} ({len(df_export_pd):,} rows, cap={parquet_fallback_max_rows:,})")

    # All actions on the cached feature frame (LDA fit/transform, rollups and
    # the Parquet export) are complete: release executor memory explicitly.
    df_features.unpersist()

    # Build comprehensive JSON payload for MongoDB and Visualizers
    analytics_payload = {
        "metadata": {
            "execution_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "input_dataset": input_csv_path,
            "label_mode": "auto" if auto_labels else "majority-category",
            "label_method": label_method,
            "topic_labels": {str(k): v for k, v in active_labels.items()},
            "label_diagnostics": {str(k): v for k, v in label_diagnostics.items()},
            "total_articles_processed": valid_count,
            "vocabulary_size": len(vocabulary),
            "num_topics": num_topics,
            "elapsed_seconds": round(time.time() - start_time, 2)
        },
        "topics": extracted_topics,
        "topic_summary": topic_summary.to_dict(orient="records"),
        "temporal_trends": temporal_trends.to_dict(orient="records"),
        "publisher_distribution": topic_by_pub.to_dict(orient="records")
    }

    os.makedirs(os.path.dirname(output_summary_json) or ".", exist_ok=True)
    with open(output_summary_json, "w", encoding="utf-8") as f:
        json.dump(analytics_payload, f, indent=2)
    print(f"[+] Analytics summary exported to {output_summary_json}")

    total_duration = time.time() - start_time
    print(f"\n=======================================================")
    print(f" PIPELINE COMPLETED SUCCESSFULLY IN {total_duration:.2f} SECONDS")
    print(f"=======================================================\n")

    spark.stop()
    return analytics_payload

if __name__ == "__main__":
    # Command-line interface lets the SAME pipeline process either the canonical
    # synthetic demo corpus (defaults) or a real full-text corpus, e.g.:
    #   python src/spark_pipeline.py \
    #       --input-csv data/real_news/raw_news_articles_real.csv \
    #       --output-parquet data/real_news/processed_parquet \
    #       --output-json data/real_news/pipeline_analytics_summary.json \
    #       --auto-labels
    parser = argparse.ArgumentParser(
        description="Distributed News Article Processing & Topic Extraction (PySpark)")
    parser.add_argument("--input-csv", default="data/raw_news_articles.csv",
                        help="Input article CSV (12-column canonical schema).")
    parser.add_argument("--output-parquet", default="data/processed_parquet",
                        help="Destination for the partitioned Parquet lakehouse.")
    parser.add_argument("--output-json", default="data/pipeline_analytics_summary.json",
                        help="Destination for the analytics summary JSON.")
    parser.add_argument("--topics", type=int, default=6,
                        help="Number of latent LDA topics (k).")
    parser.add_argument("--max-iterations", type=int, default=20,
                        help="LDA training iterations.")
    parser.add_argument("--vocab-size", type=int, default=5000,
                        help="Maximum vocabulary size for CountVectorizer.")
    parser.add_argument("--auto-labels", action="store_true",
                        help="Derive topic labels from learned top terms "
                             "(recommended for real-world corpora).")
    args = parser.parse_args()

    run_distributed_pipeline(
        input_csv_path=args.input_csv,
        output_parquet_dir=args.output_parquet,
        output_summary_json=args.output_json,
        num_topics=args.topics,
        max_iterations=args.max_iterations,
        vocab_size=args.vocab_size,
        auto_labels=args.auto_labels,
    )
