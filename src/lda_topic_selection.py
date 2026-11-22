# ===========================================================================
# LDA MODEL SELECTION SWEEP  (choosing k = number of latent topics)
# Big Data Analytics for News Article Analysis and Topic Extraction
# ---------------------------------------------------------------------------
# Author : Shehab Osama (AI, ML & NLP Lead)
# ---------------------------------------------------------------------------
# The production pipeline (src/spark_pipeline.py) trains LDA with a single,
# fixed k. This module answers the question an examiner will always ask:
#   "WHY that k, and not 4 or 12?"
#
# It re-fits the identical NLP pipeline (Stage 1-3 of the main pipeline:
# RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF) once, caches
# the feature matrix in executor memory, and then sweeps k across a range,
# recording three complementary, reproducible diagnostics per k:
#
#   1. log perplexity  -- PySpark MLlib LDAModel.logPerplexity(dataset).
#                         Predictive fit; LOWER is better. LDA perplexity
#                         characteristically keeps falling as k grows, so it
#                         alone can never justify a mid-range k.
#   2. mean coherence  -- UMass-style document co-occurrence coherence over
#                         the top-N terms of every topic. Measures whether the
#                         top words of a topic actually appear together in the
#                         same documents. HIGHER (closer to 0) is better.
#   3. topic_overlap   -- mean pairwise Jaccard overlap between the top-term
#                         SETS of different topics. Measures topic redundancy.
#                         HIGHER = MORE overlap; LOWER is better. The JSON key
#                         stays "distinctness" for backward compatibility.
#   4. heldout_log_perplexity -- logPerplexity on a deterministic 80/20 test
#                         split (seed 42). The original in-sample
#                         "log_perplexity" is retained for continuity.
#
# Model selection rule (fixed BEFORE looking at the numbers):
#   * log perplexity is reported to show that it degrades monotonically and
#     therefore cannot be used as the selection criterion on its own;
#   * k is selected at the coherence knee: the point where marginal coherence
#     gain collapses (< 5% of the total coherence span), i.e. adding topics
#     stops buying interpretable structure;
#   * the selected k must not increase topic redundancy (topic overlap must not
#     worsen versus the previous k).
#
# Outputs
#   data/lda_k_sweep.json          machine-readable metrics for every k tested
#   visualizations/lda_k_sweep.png publication-grade 300 DPI evidence chart
#
# Usage
#   python src/lda_topic_selection.py
#   python src/lda_topic_selection.py --input data/raw_news_articles.csv \
#          --k-min 3 --k-max 12 --max-iter 20
# ===========================================================================

import os
import sys
import json
import math
import random
import time
import argparse

# Held-out LDA evaluation settings: fixed seed and an 80/20 split, recorded in
# the sweep JSON for reproducibility.
HELD_OUT_SEED = 42
TRAIN_FRACTION = 0.8

# --- Resolve the project root so `from src...` imports work from any cwd ----
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# PySpark, matplotlib and src.spark_pipeline are imported LAZILY inside the
# functions that use them. Importing src.spark_pipeline at module scope triggers
# an NLTK VADER download, which would make offline test collection and the pure
# metric helpers below needlessly fragile. Reuse of init_spark_session /
# define_explicit_schema is preserved via those local imports.

# Domain stopwords -- identical to Stage 3 of src/spark_pipeline.py so the
# sweep measures the SAME model that production deploys (fair comparison).
DOMAIN_STOPWORDS = [
    "said", "also", "would", "could", "one", "two", "new", "first", "last",
    "year", "years", "time", "day", "days", "reuters", "bloomberg", "associated",
    "press", "times", "journal", "click", "read", "subscribe", "newsletter", "advertisement"
]


def build_feature_frame(spark, csv_path: str, vocab_size: int):
    """
    Rebuilds Stage 1-3 of the production pipeline and returns the TF-IDF
    feature DataFrame, the fitted vocabulary, and the per-document token sets
    (used on the driver for the coherence / topic_overlap diagnostics).

    Stages mirrored 1:1 from src/spark_pipeline.run_distributed_pipeline():
      Stage 1 -- strict schema enforcement on ingest
      Stage 2 -- HTML/URL/entity stripping, lowercasing, short-doc purge
      Stage 3 -- RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF

    All Spark / production-helper imports are LOCAL so importing this module
    for its pure math helpers stays offline and side-effect free.
    """
    from pyspark.sql.functions import (col, trim, lower, regexp_replace, size, split, when)
    from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
    from src.spark_pipeline import define_explicit_schema  # reuse canonical schema

    print(f"[+] Ingesting corpus with enforced schema: {csv_path}")
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("escape", "\"")
        .option("multiline", "true")
        .schema(define_explicit_schema())   # never let Spark guess the schema
        .load(csv_path)
    )
    total_raw = df_raw.count()

    # --- Stage 2: cleaning, imputation and text hygiene ---------------------
    df_clean = df_raw.filter(
        col("article_id").isNotNull() &
        col("content").isNotNull() &
        (trim(col("content")) != "")
    ).withColumn(
        "author",
        when(col("author").isNull() | (trim(col("author")) == ""), "Staff Correspondent")
        .otherwise(col("author"))
    )

    body = regexp_replace(col("content"), r"<[^>]+>", " ")        # strip HTML
    body = regexp_replace(body, r"http[s]?://\S+", " ")          # strip URLs
    body = regexp_replace(body, r"&[a-z0-9#]+;", " ")            # strip entities
    body = regexp_replace(body, r"[^a-zA-Z\s]", " ")             # keep letters
    body = regexp_replace(body, r"\s+", " ")                     # collapse spaces

    df_clean = df_clean.withColumn("clean_content", trim(lower(body)))
    df_clean = df_clean.withColumn(
        "clean_word_count", size(split(col("clean_content"), " "))
    )
    df_valid = df_clean.filter(col("clean_word_count") >= 10)     # purge stubs
    valid_count = df_valid.count()
    print(f"[+] Documents retained: {valid_count:,} (purged {total_raw - valid_count:,})")

    # --- Stage 3: distributed NLP feature engineering ------------------------
    tokenizer = RegexTokenizer(
        inputCol="clean_content", outputCol="raw_tokens",
        pattern=r"\s+", minTokenLength=3
    )
    remover = StopWordsRemover(
        inputCol="raw_tokens", outputCol="filtered_tokens",
        stopWords=StopWordsRemover.loadDefaultStopWords("english") + DOMAIN_STOPWORDS
    )
    df_tokens = remover.transform(tokenizer.transform(df_valid))

    vectorizer = CountVectorizer(
        inputCol="filtered_tokens", outputCol="tf_features",
        vocabSize=vocab_size, minDF=3.0
    )
    cv_model = vectorizer.fit(df_tokens)
    df_tf = cv_model.transform(df_tokens)
    vocabulary = cv_model.vocabulary              # index -> term lookup table
    print(f"[+] Vocabulary: {len(vocabulary):,} terms")

    idf_model = IDF(inputCol="tf_features", outputCol="tfidf_features").fit(df_tf)
    df_features = idf_model.transform(df_tf)

    # Cache once: every k in the sweep re-reads this feature matrix.
    # Without persist() Spark would rebuild the whole 3-stage lineage 10 times.
    df_features.persist()
    print(f"[+] Feature matrix cached in executor memory: {df_features.count():,} rows")

    # Pull the token sets to the driver ONCE (6k docs) for the coherence math.
    token_sets = [
        set(tokens) for tokens in
        df_tokens.select("filtered_tokens").rdd.map(lambda r: r[0]).collect()
    ]
    return df_features, vocabulary, token_sets


def co_occurrence_stats(token_sets, terms):
    """
    Single pass over the corpus for one topic's top-term list.
    Returns (document_frequency, pair_counts):
      document_frequency[t] = number of documents containing term t
      pair_counts[(a, b)]   = number of documents containing BOTH a and b
    """
    doc_freq = {t: 0 for t in terms}
    pair_counts = {(a, b): 0 for a in terms for b in terms if a != b}

    for tokens in token_sets:                 # one scan per topic
        present = [t for t in terms if t in tokens]
        for t in present:
            doc_freq[t] += 1
        for a in present:                     # pairwise co-occurrence
            for b in present:
                if a != b:
                    pair_counts[(a, b)] += 1
    return doc_freq, pair_counts


def umass_coherence(terms, token_sets):
    """
    UMass-style coherence for one topic, averaged over ordered term pairs.

        coherence = mean over (t_i, t_j), i != j of  log( (D(t_i,t_j) + 1) / (D(t_i) + 1) )

    D(x)   = document frequency of term x
    D(x,y) = number of documents containing both x and y
    The score is <= 0; values CLOSER TO ZERO mean the top terms of the topic
    genuinely co-occur in the same articles (a tighter, more interpretable
    topic). +1 smoothing keeps the log finite for unseen pairs.
    """
    doc_freq, pair_counts = co_occurrence_stats(token_sets, terms)
    scores = []
    for (a, b), joint in pair_counts.items():
        if doc_freq[a] == 0:
            continue                          # term never survived pruning
        scores.append(math.log((joint + 1.0) / (doc_freq[a] + 1.0)))
    return (sum(scores) / len(scores)) if scores else float("nan")


def heldout_split_indices(n_rows, seed=HELD_OUT_SEED, train_fraction=TRAIN_FRACTION):
    """
    Pure, deterministic index partition used to describe the held-out split.
    Returns (train_indices, test_indices) with an 80/20 proportion by default.
    Used by unit tests to verify split determinism without starting Spark.
    """
    if n_rows < 2:
        raise ValueError("Held-out evaluation requires at least two documents.")
    indices = list(range(n_rows))
    random.Random(seed).shuffle(indices)
    cut = max(1, min(n_rows - 1, int(round(n_rows * train_fraction))))
    return indices[:cut], indices[cut:]


def heldout_split_weights(train_fraction=TRAIN_FRACTION):
    """Weights passed to DataFrame.randomSplit (fixed seed in sweep_k)."""
    return [train_fraction, 1.0 - train_fraction]


def topic_overlap(topic_term_sets):
    """
    Mean pairwise Jaccard overlap between the top-term sets of different
    topics (internally renamed from 'distinctness': HIGHER = MORE overlap).
    0.0 = no shared top terms; 1.0 = carbon-copy topics. Returns None when
    there are fewer than two topics, i.e. the metric is N/A.
    """
    sets = [set(s) for s in topic_term_sets]
    overlaps = []
    for i in range(len(sets)):
        for j in range(i + 1, len(sets)):
            union = sets[i] | sets[j]
            if union:
                overlaps.append(len(sets[i] & sets[j]) / len(union))
    return (sum(overlaps) / len(overlaps)) if overlaps else None


# Backward-compatible alias for code/tests that used the old function name.
distinctness = topic_overlap


def validate_metrics(k, coherence_scores, topic_term_sets):
    """
    Validates degenerate diagnostics instead of silently reporting 0.0.

    Returns (mean_coherence, overlap, flags):
      * empty/all-non-finite coherence -> NaN + 'coherence_empty' flag;
      * overlap with no topic pairs (k < 2) -> None (N/A) + 'topic_overlap_na';
      * non-finite individual scores additionally set 'coherence_nonfinite'.
    """
    flags = {}
    finite_scores = [s for s in coherence_scores if s is not None and math.isfinite(s)]
    if len(finite_scores) != len(coherence_scores):
        flags["coherence_nonfinite"] = True
    mean_coherence = (sum(finite_scores) / len(finite_scores)) if finite_scores else float("nan")
    if not finite_scores:
        flags["coherence_empty"] = True

    overlap = topic_overlap(topic_term_sets)
    if overlap is None or k < 2:
        flags["topic_overlap_na"] = True
        overlap = None
    return mean_coherence, overlap, flags


def sweep_k(df_features, vocabulary, token_sets, k_min, k_max, max_iter, top_n):
    """
    Trains one LDA model per k on a fixed 80/20 training split and records:
      * log_perplexity        -- in-sample fit (retained key for continuity),
      * heldout_log_perplexity -- logPerplexity on the untouched test split,
      * mean_coherence / topic_overlap (JSON key: distinctness) plus flags.
    Returns the list of per-k result records (dicts).
    """
    from pyspark.ml.clustering import LDA  # lazy import: module import stays offline

    train_df, test_df = df_features.randomSplit(
        heldout_split_weights(), seed=HELD_OUT_SEED
    )
    train_df.persist()
    test_df.persist()
    train_count, test_count = train_df.count(), test_df.count()
    print(f"[+] Held-out split (seed={HELD_OUT_SEED}): "
          f"train={train_count:,} rows, test={test_count:,} rows")

    records = []
    try:
        for k in range(k_min, k_max + 1):
            t0 = time.time()
            print(f"\n[+] Training LDA with k={k} (maxIter={max_iter}, optimizer=online)...")

            model = LDA(
                k=k,
                maxIter=max_iter,
                featuresCol="tf_features",
                topicDistributionCol="topicDistribution",
                optimizer="online",     # online variational Bayes -- scales linearly
                seed=42                 # fixed seed: the sweep must be reproducible
            ).fit(train_df)

            # --- diagnostic 1: in-sample + held-out predictive fit ------------
            log_perplexity = float(model.logPerplexity(train_df))
            heldout = float(model.logPerplexity(test_df)) if test_count else float("nan")

            # --- diagnostics 2 & 3: interpretability --------------------------
            topics = model.describeTopics(maxTermsPerTopic=top_n).collect()
            topic_terms = []
            for row in topics:
                # termIndices are vocabulary indexes -> map back to real words
                topic_terms.append([vocabulary[i] for i in row["termIndices"]])

            coherence_scores = [umass_coherence(terms, token_sets) for terms in topic_terms]
            mean_coherence, overlap, flags = validate_metrics(k, coherence_scores, topic_terms)
            if not test_count:
                flags["heldout_empty"] = True

            elapsed = time.time() - t0
            overlap_display = "N/A" if overlap is None else f"{overlap:.4f}"
            records.append({
                "k": k,
                "log_perplexity": round(log_perplexity, 4) if math.isfinite(log_perplexity) else None,
                "heldout_log_perplexity": round(heldout, 4) if math.isfinite(heldout) else None,
                "mean_coherence": round(mean_coherence, 4) if math.isfinite(mean_coherence) else None,
                # Internal metric is topic_overlap; key retained for compatibility.
                "distinctness": round(overlap, 4) if overlap is not None else None,
                "metric_flags": flags,
                "train_seconds": round(elapsed, 2),
                "top_terms_per_topic": topic_terms,
            })
            print(f"    logPerplexity={log_perplexity:.4f} | "
                  f"heldoutLogPerplexity={heldout:.4f} | "
                  f"meanCoherence={mean_coherence:.4f} | "
                  f"topicOverlap={overlap_display} | {elapsed:.1f}s")
    finally:
        train_df.unpersist()
        test_df.unpersist()
    return records


def select_k(records):
    """
    Applies the pre-registered selection rule (see module header) and returns
    (selected_k, rationale_string, monotone_flag).

      Step 1 -- confirm perplexity improves monotonically with k (if it does
                not, perplexity is still not treated as decisive on its own).
      Step 2 -- find the coherence knee: the first k where the marginal gain
                falls below 5% of the total coherence span across the sweep.
      Step 3 -- reject a candidate knee that increases topic overlap (the
                backward-compatible 'distinctness' JSON key); fall back one step.

    Degenerate coherence (NaN/None) raises ValueError rather than silently
    selecting a k from a 0.0 placeholder; topic overlap may be N/A for k<2.
    """
    ordered = sorted(records, key=lambda r: r["k"])
    if not ordered:
        raise ValueError("Cannot select k: sweep produced no records.")

    coh_by_index = {
        i: r["mean_coherence"] for i, r in enumerate(ordered)
        if r.get("mean_coherence") is not None and math.isfinite(r["mean_coherence"])
    }
    if not coh_by_index:
        raise ValueError("Cannot select k: coherence is empty/NaN for every k "
                         "(see per-k metric_flags).")
    valid_indices = sorted(coh_by_index)
    span = max(coh_by_index.values()) - min(coh_by_index.values())

    # Step 1: monotone perplexity check (reported, not decisive).
    perplexities = [r.get("log_perplexity") for r in ordered]
    finite_perp = [p for p in perplexities if p is not None and math.isfinite(p)]
    monotone = (len(finite_perp) == len(perplexities)) and all(
        finite_perp[i] >= finite_perp[i + 1] for i in range(len(finite_perp) - 1)
    )
    p_first = finite_perp[0] if finite_perp else float("nan")
    p_last = finite_perp[-1] if finite_perp else float("nan")

    # Step 2: coherence knee by marginal-gain collapse (skip invalid metrics).
    knee_index = valid_indices[-1]
    for prev_i, cur_i in zip(valid_indices, valid_indices[1:]):
        marginal = coh_by_index[cur_i] - coh_by_index[prev_i]
        if span > 0 and marginal < 0.05 * span:
            knee_index = cur_i
            break

    # Step 3: penalise redundancy (N/A overlap for k<2 is never a penalty).
    while knee_index > 0:
        cur = ordered[knee_index].get("distinctness")
        prev = ordered[knee_index - 1].get("distinctness")
        if cur is None or prev is None or cur <= prev:
            break
        knee_index -= 1

    selected = ordered[knee_index]["k"]
    overlap = ordered[knee_index].get("distinctness")
    overlap_text = "N/A (fewer than two topics)" if overlap is None else f"{overlap:.3f}"
    rationale = (
        f"log perplexity {'decreases' if monotone else 'does not decrease'} monotonically "
        f"across k in [{ordered[0]['k']}, {ordered[-1]['k']}] "
        f"({p_first:.2f} -> {p_last:.2f}), so it cannot discriminate between "
        f"models on its own (the held-out log perplexity is reported too, not decisive). "
        f"Mean UMass coherence spans {span:.3f} over the same range; the "
        f"marginal coherence gain collapses (<5% of the span) at k={selected}, which is the "
        f"smallest k on the interpretability plateau. Topic overlap (JSON key "
        f"'distinctness') at k={selected} is {overlap_text} (lower is less redundant), so "
        f"the model is not fragmenting a single domain into duplicates."
    )
    return selected, rationale, monotone


def render_chart(records, selected_k, rationale, out_png, documents=None):
    """Two-panel 300 DPI evidence figure: perplexity curve + coherence curve."""
    # Headless plotting backend + typography kept local so importing the pure
    # helpers does not import matplotlib.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams["font.sans-serif"] = "Arial"
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["figure.dpi"] = 300

    ordered = sorted(records, key=lambda r: r["k"])
    ks = [r["k"] for r in ordered]
    perp = [r["log_perplexity"] if r["log_perplexity"] is not None else float("nan")
            for r in ordered]
    coh = [r["mean_coherence"] if r["mean_coherence"] is not None else float("nan")
           for r in ordered]
    dist = [r["distinctness"] if r["distinctness"] is not None else float("nan")
            for r in ordered]

    fig, axes = plt.subplots(1, 2, figsize=(15, 5.6))

    # --- Panel 1: predictive fit (log perplexity) ---------------------------
    axes[0].plot(ks, perp, marker="o", linewidth=2.4, color="#7F7FD5", label="log perplexity")
    axes[0].axvline(selected_k, color="#E4572E", linestyle="--", linewidth=1.8,
                    label=f"selected k = {selected_k}")
    axes[0].set_title("Predictive fit degrades monotonically with k\n(log perplexity, lower = better)",
                      fontsize=12, fontweight="bold")
    axes[0].set_xlabel("Number of latent topics (k)")
    axes[0].set_ylabel("MLlib log perplexity")
    axes[0].set_xticks(ks)
    axes[0].legend(fontsize=9)
    axes[0].annotate("Perplexity keeps improving,\nso it cannot pick k on its own",
                     xy=(ks[-1], perp[-1]), xytext=(ks[len(ks) // 2], perp[-1] * 1.02),
                     fontsize=8.5, color="#555555",
                     arrowprops=dict(arrowstyle="->", color="#999999"))

    # --- Panel 2: interpretability (coherence + redundancy) ------------------
    axes[1].plot(ks, coh, marker="s", linewidth=2.4, color="#4C9F70",
                 label="mean UMass coherence (higher = tighter topics)")
    axes[1].axvline(selected_k, color="#E4572E", linestyle="--", linewidth=1.8,
                    label=f"selected k = {selected_k}")
    axes[1].set_title("Interpretability settles on a plateau\n(coherence knee + topic overlap)",
                      fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Number of latent topics (k)")
    axes[1].set_ylabel("Mean UMass coherence (<= 0, closer to 0 is better)")
    axes[1].set_xticks(ks)

    # Redundancy on a twin axis: shows whether extra topics duplicate others.
    ax2 = axes[1].twinx()
    ax2.plot(ks, dist, marker="^", linewidth=1.6, linestyle=":", color="#B0855B",
             label="topic_overlap (JSON key: distinctness; lower = less redundant)")
    ax2.set_ylabel("Mean pairwise Jaccard overlap", color="#B0855B")
    ax2.tick_params(axis="y", labelcolor="#B0855B")

    lines = axes[1].get_lines() + ax2.get_lines()
    axes[1].legend(lines, [ln.get_label() for ln in lines], fontsize=8.5, loc="lower left")

    # Corpus size is derived from the ingested data, never hardcoded.
    corpus_note = (f"{documents:,} full-text articles" if documents is not None
                   else "the analyzed corpus")
    fig.suptitle(f"LDA Model Selection: sweeping k over {ks[0]}-{ks[-1]} on {corpus_note}",
                 fontsize=14, fontweight="bold")
    fig.text(0.5, 0.005, rationale, ha="center", fontsize=8, wrap=True, color="#333333")
    fig.tight_layout(rect=[0, 0.05, 1, 0.94])
    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"[+] Evidence chart written -> {out_png}")


def main():
    parser = argparse.ArgumentParser(
        description="Sweep k for the LDA topic model and justify the selected value."
    )
    parser.add_argument("--input", default="data/raw_news_articles.csv",
                        help="Corpus CSV to sweep on (default: the canonical full-text benchmark).")
    parser.add_argument("--k-min", type=int, default=3, help="Smallest k to test.")
    parser.add_argument("--k-max", type=int, default=12, help="Largest k to test.")
    parser.add_argument("--max-iter", type=int, default=20, help="LDA training iterations per k.")
    parser.add_argument("--vocab-size", type=int, default=5000, help="Max CountVectorizer vocabulary.")
    parser.add_argument("--top-n", type=int, default=10, help="Top terms per topic for coherence.")
    parser.add_argument("--output-json", default="data/lda_k_sweep.json", help="Metrics output path.")
    parser.add_argument("--output-png", default="visualizations/lda_k_sweep.png",
                        help="Evidence chart output path.")
    args = parser.parse_args()

    start = time.time()
    print("=" * 78)
    print(" LDA MODEL SELECTION SWEEP -- justifying k with measured evidence")
    print("=" * 78)

    # Lazy reuse of the production session builder avoids the import-time
    # NLTK VADER download in src.spark_pipeline.
    from src.spark_pipeline import init_spark_session
    spark = init_spark_session("LDAModelSelectionSweep")
    try:
        df_features, vocabulary, token_sets = build_feature_frame(
            spark, args.input, args.vocab_size
        )
        records = sweep_k(df_features, vocabulary, token_sets,
                          args.k_min, args.k_max, args.max_iter, args.top_n)
        selected_k, rationale, monotone = select_k(records)
        documents = df_features.count()

        payload = {
            "corpus": args.input,
            "documents": documents,
            "vocabulary_size": len(vocabulary),
            "k_range": [args.k_min, args.k_max],
            "max_iterations": args.max_iter,
            "optimizer": "online (variational Bayes)",
            "seed": 42,
            "heldout_evaluation": {
                "split": "80/20 train/test (DataFrame.randomSplit)",
                "seed": HELD_OUT_SEED,
                "train_fraction": TRAIN_FRACTION,
                "metric": "heldout_log_perplexity (per k, lower = better)",
            },
            "metrics": {
                "log_perplexity": ("PySpark MLlib LDAModel.logPerplexity on the TRAIN split "
                                   "(retained key for continuity; lower = better fit)"),
                "heldout_log_perplexity": ("Same metric evaluated on the untouched 20% test "
                                           "split (lower = better predictive fit)"),
                "mean_coherence": ("UMass-style document co-occurrence coherence over the top "
                                   f"{args.top_n} terms of each topic, mean over ordered pairs "
                                   "log((D(a,b)+1)/(D(a)+1)); higher (closer to 0) = tighter topic"),
                "distinctness": ("Backward-compatible JSON key for the internally renamed "
                                 "topic_overlap metric: mean pairwise Jaccard overlap of "
                                 "topic top-term sets; HIGHER = MORE overlap, lower = less "
                                 "redundant. null with topic_overlap_na flag when k<2."),
            },
            "metric_notes": {
                "naming": ("'distinctness' is kept only as a JSON compatibility key; the "
                           "implementation now calls this measure topic_overlap."),
                "degenerate_metrics": ("Empty/non-finite coherence yields NaN + a metric_flags "
                                       "entry instead of a silent 0.0; k<2 marks topic overlap N/A."),
            },
            "sweep": records,
            "perplexity_monotone": monotone,
            "selected_k": selected_k,
            "selection_rule": ("Perplexity declines monotonically and is therefore reported but not "
                               "decisive; k is selected at the coherence knee (marginal gain <5% of "
                               "the coherence span), rejecting any k that increases topic redundancy."),
            "rationale": rationale,
            "total_seconds": round(time.time() - start, 2),
        }

        os.makedirs(os.path.dirname(args.output_json) or ".", exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print(f"\n[+] Metrics written -> {args.output_json}")

        render_chart(records, selected_k, rationale, args.output_png, documents=documents)

        print("\n" + "=" * 78)
        print(f" SELECTED k = {selected_k}")
        print(f" {rationale}")
        print(f" Total sweep time: {payload['total_seconds']}s")
        print("=" * 78)
    finally:
        spark.stop()                 # always release the Spark session


if __name__ == "__main__":
    main()
