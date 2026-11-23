"""
Intelligence Command Center Dashboard Builder
==============================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)

Purpose
-------
Builds ``dashboard/command_center.html``: a single-file, dark-mode,
"Intelligence Command Center" dashboard that runs entirely in the browser
(no CDN, no server, no build step at view time).

Design notes
------------
- The payload is generated from **committed project artifacts only**:
  * ``data/pipeline_analytics_summary.json``            (synthetic corpus)
  * ``data/real_news/pipeline_analytics_summary.json``  (Reuters corpus)
  * ``data/hadoop_ecosystem/hbase/news_articles.json``  (article-level NLP)
  * ``data/raw_news_articles.csv``                      (full text / snippets)
  * ``data/real_news/raw_news_articles_real.csv``       (real wire sample)
- Topic volumes, sentiments, confidences and monthly series are the measured
  PySpark outputs. Nothing here is hand-typed.
- Article-level rows for the Reuters corpus are assigned to the nearest learned
  topic by counting that topic's top terms in the text. This is a display
  heuristic (documented in the UI) because the real corpus summary is
  aggregate-only; the synthetic corpus rows carry the model's own
  ``dominant_topic`` / ``topic_confidence`` fields.
- Named entities are produced by a transparent gazetteer over article titles
  and full content. No spaCy model is required, and the UI states the method.
- Article sentiment for the Reuters sample is VADER compound; the synthetic
  rows use the pipeline's stored ``sentiment_polarity``.

Run:
    python src/build_command_center.py
"""

import csv
import datetime as dt
import json
import os
import re
import sys
from collections import Counter, defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:  # allows `python src/build_command_center.py`
    sys.path.insert(0, PROJECT_ROOT)

SYNTH_SUMMARY = os.path.join(PROJECT_ROOT, "data", "pipeline_analytics_summary.json")
REAL_SUMMARY = os.path.join(PROJECT_ROOT, "data", "real_news",
                            "pipeline_analytics_summary.json")
HBASE_ARTICLES = os.path.join(PROJECT_ROOT, "data", "hadoop_ecosystem", "hbase",
                              "news_articles.json")
SYNTH_CSV = os.path.join(PROJECT_ROOT, "data", "raw_news_articles.csv")
REAL_CSV = os.path.join(PROJECT_ROOT, "data", "real_news",
                        "raw_news_articles_real.csv")
SCALE_BENCHMARK = os.path.join(PROJECT_ROOT, "data", "scale_benchmark.json")
TEMPLATE = os.path.join(PROJECT_ROOT, "dashboard", "command_center.template.html")
OUTPUT = os.path.join(PROJECT_ROOT, "dashboard", "command_center.html")

# Editorial display names for the auto-derived Reuters topic labels. The raw
# label stays visible in the UI so nothing is hidden; the alias only makes the
# cluster map readable. Every alias is a direct paraphrase of its learned terms.
DISPLAY_ALIASES = {
    "Mln / Dlrs / Net": "Earnings & Dollar Flows",
    "Company / Shares / Stock": "Equities & Corporate",
    "Pct / Bank / Rate": "Rates & Banking",
    "Trade / Japan / Market": "Trade & Asia Markets",
    "Mln / Tonnes / Pct": "Commodities & Tonnage",
    "Nil / Prev / Usda": "USDA Crop Reports",
}

# Named-entity gazetteer. Publications and bylines are added from the data at
# runtime; these are the organisations and places that actually occur in the
# committed headlines.
ORG_GAZETTEER = [
    "Federal Reserve", "European Union", "White House", "United Nations",
    "Congress", "Pentagon", "NASA", "WHO", "OPEC", "NATO", "SEC", "FBI",
    "CIA", "OpenAI", "Google", "Apple", "Microsoft", "Amazon", "Meta",
    "Tesla", "Nvidia", "Boeing", "Goldman Sachs", "JPMorgan", "Reuters",
]
LOC_GAZETTEER = [
    "United States", "U.S.", "China", "Japan", "Germany", "France", "Britain",
    "UK", "Russia", "Ukraine", "India", "Brazil", "Canada", "Mexico",
    "Australia", "Europe", "Asia", "Africa", "Washington", "London",
    "Beijing", "Tokyo", "Moscow", "Brussels", "New York", "California",
]

SENT_POS = 0.05
SENT_NEG = -0.05


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def slug(value):
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def clean_text(value, limit=None):
    text = re.sub(r"\s+", " ", (value or "")).strip()
    if limit and len(text) > limit:
        text = text[: limit - 1].rstrip() + "\u2026"
    return text


def sentiment_bucket(value):
    if value is None:
        return "neutral"
    if value > SENT_POS:
        return "positive"
    if value < SENT_NEG:
        return "negative"
    return "neutral"


def token_set(text):
    return set(re.findall(r"[a-z0-9]+", (text or "").lower()))


def term_overlap(label_tokens, terms):
    """Loose overlap so 'genomics' matches 'genomic' and 'mln' matches 'mln'."""
    score = 0
    for token in label_tokens:
        stem = token[:5]
        for term in terms:
            if term == token or term.startswith(stem) or token.startswith(term[:5]):
                score += 1
                break
    return score


# ---------------------------------------------------------------------------
# Topic catalogue
# ---------------------------------------------------------------------------
def build_topics():
    topics = []
    for corpus, path in (("synthetic", SYNTH_SUMMARY), ("reuters", REAL_SUMMARY)):
        summary = load_json(path)
        components = []
        for key, value in summary["topics"].items():
            components.append((key, value.get("terms", [])))

        for row in summary["topic_summary"]:
            raw_label = row["topic_label"]
            label_tokens = token_set(raw_label)
            best_terms = []
            best_score = -1
            for _key, terms in components:
                score = term_overlap(label_tokens, terms)
                if score > best_score:
                    best_score = score
                    best_terms = terms
            display = DISPLAY_ALIASES.get(raw_label, raw_label)
            topics.append({
                "id": slug(f"{corpus}-{raw_label}"),
                "label": display,
                "raw_label": raw_label,
                "corpus": corpus,
                "volume": int(row["total_articles"]),
                "sentiment": round(float(row["mean_sentiment"]), 4),
                "confidence": round(float(row["mean_confidence"]), 4),
                "mean_length": round(float(row["mean_length"]), 1),
                "terms": [t for t in best_terms[:12]],
            })
    return topics


def topic_id_for_label(topics, corpus, raw_label):
    for topic in topics:
        if topic["corpus"] == corpus and topic["raw_label"] == raw_label:
            return topic["id"]
    return None


# ---------------------------------------------------------------------------
# Monthly timeline
# ---------------------------------------------------------------------------
def build_timeline(topics):
    months = []
    year, month = 2024, 1
    while (year, month) <= (2025, 12):
        months.append(f"{year}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1

    timeline = {ym: {"total": 0, "sentiment": 0.0, "by_topic": {}} for ym in months}

    for corpus, path in (("synthetic", SYNTH_SUMMARY), ("reuters", REAL_SUMMARY)):
        summary = load_json(path)
        for row in summary["temporal_trends"]:
            ym = f"{int(row['year'])}-{int(row['month']):02d}"
            if ym not in timeline:
                continue
            tid = topic_id_for_label(topics, corpus, row["topic_label"])
            if tid is None:
                continue
            count = int(row["article_count"])
            sentiment = float(row["avg_sentiment"])
            timeline[ym]["by_topic"][tid] = {
                "count": count,
                "sentiment": round(sentiment, 4),
            }
            timeline[ym]["total"] += count
            timeline[ym]["sentiment"] += sentiment * count

    for ym, bucket in timeline.items():
        bucket["sentiment"] = (round(bucket["sentiment"] / bucket["total"], 4)
                               if bucket["total"] else 0.0)
    return months, timeline


# ---------------------------------------------------------------------------
# Articles
# ---------------------------------------------------------------------------
def best_topic_for_text(text, topics):
    lowered = (text or "").lower()
    best, best_score = None, 0
    for topic in topics:
        terms = [t.lower() for t in topic["terms"][:12]]
        score = sum(lowered.count(term) for term in terms)
        if score > best_score:
            best, best_score = topic["id"], score
    return best


def load_synth_articles(topics, per_missing_month=50):
    raw_rows = []
    with open(SYNTH_CSV, "r", encoding="utf-8", newline="") as handle:
        raw_rows = list(csv.DictReader(handle))
    raw_by_id = {row["article_id"]: row for row in raw_rows}

    hbase = load_json(HBASE_ARTICLES)
    articles = []
    seen = set()
    month_counts = Counter()
    for key, record in hbase.items():
        article_id = key.split("_", 1)[1] if "_" in key else key
        meta = record.get("cf_meta", {})
        nlp = record.get("cf_nlp", {})
        source_row = raw_by_id.get(article_id, {})
        label = nlp.get("topic_label", "")
        tid = topic_id_for_label(topics, "synthetic", label)
        if tid is None:
            continue
        date = meta.get("date", source_row.get("date", ""))
        articles.append({
            "id": article_id,
            "title": clean_text(meta.get("title"), 180),
            "source": meta.get("publication", source_row.get("publication", "")),
            "author": meta.get("author", source_row.get("author", "")),
            "date": date,
            "topic": tid,
            "confidence": round(float(nlp.get("topic_confidence", 0.0)), 4),
            "sentiment": round(float(nlp.get("sentiment_polarity", 0.0)), 4),
            "corpus": "synthetic",
            "label_method": "lda",
            "snippet": clean_text(source_row.get("content"), 230),
            "_entity_text": source_row.get("content", ""),
            "url": record.get("cf_raw", {}).get("url", source_row.get("url", "")),
        })
        seen.add(article_id)
        month_counts[date[:7]] += 1

    # HBase stores a 2,000-row sample, which leaves 2024-07..09 and all of 2025
    # empty. Top each thin month up from the raw CSV so timeline brushing never
    # lands on an empty feed. Supplemental rows use a learned-term topic match
    # and VADER sentiment, and are labelled as such in the drawer.
    synth_topics = [t for t in topics if t["corpus"] == "synthetic"]
    by_month = defaultdict(list)
    for row in raw_rows:
        if row["article_id"] in seen:
            continue
        by_month[row["date"][:7]].append(row)

    vader = build_vader()
    for month, rows in sorted(by_month.items()):
        need = per_missing_month - month_counts.get(month, 0)
        if need <= 0:
            continue
        step = max(1, len(rows) // need)
        for row in rows[::step][:need]:
            text = f"{row.get('title', '')} {row.get('content', '')}"
            tid = best_topic_for_text(text, synth_topics)
            if tid is None:
                continue
            sentiment = (vader.polarity_scores(text[:2000])["compound"]
                         if vader else 0.0)
            articles.append({
                "id": row["article_id"],
                "title": clean_text(row.get("title"), 180),
                "source": row.get("publication", ""),
                "author": row.get("author", ""),
                "date": row.get("date", ""),
                "topic": tid,
                "confidence": None,
                "sentiment": round(float(sentiment), 4),
                "corpus": "synthetic",
                "label_method": "keyword",
                "snippet": clean_text(row.get("content"), 230),
                "_entity_text": row.get("content", ""),
                "url": row.get("url", ""),
            })
    return articles


def build_vader():
    try:
        import nltk
        from nltk.sentiment.vader import SentimentIntensityAnalyzer
        try:
            return SentimentIntensityAnalyzer()
        except LookupError:
            nltk.download("vader_lexicon", quiet=True)
            return SentimentIntensityAnalyzer()
    except Exception:  # pragma: no cover - optional dependency path
        return None


def load_real_articles(topics, per_topic=50, extra=60):
    rows = []
    with open(REAL_CSV, "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    real_topics = [t for t in topics if t["corpus"] == "reuters"]
    lowered = []
    for row in rows:
        text = (row.get("title", "") + " " + row.get("content", "")).lower()
        lowered.append(text)

    chosen = {}
    for topic in real_topics:
        terms = [t.lower() for t in topic["terms"][:12]]
        scored = []
        for index, text in enumerate(lowered):
            score = sum(text.count(term) for term in terms)
            if score:
                scored.append((score, index))
        scored.sort(reverse=True)
        for _score, index in scored[:per_topic]:
            chosen[index] = topic["id"]

    # A little breadth: add recent, high-scoring rows that missed the cut.
    for index in range(len(rows) - 1, max(len(rows) - extra, -1), -1):
        if index in chosen:
            continue
        best_topic, best_score = None, 0
        for topic in real_topics:
            terms = [t.lower() for t in topic["terms"][:12]]
            score = sum(lowered[index].count(term) for term in terms)
            if score > best_score:
                best_topic, best_score = topic["id"], score
        if best_topic:
            chosen[index] = best_topic

    vader = build_vader()
    articles = []
    for index, tid in sorted(chosen.items()):
        row = rows[index]
        content = row.get("content", "")
        sentiment = (vader.polarity_scores(content[:2000])["compound"]
                     if vader else 0.0)
        articles.append({
            "id": row["article_id"],
            "title": clean_text(row.get("title"), 180),
            "source": row.get("publication", "Reuters"),
            "author": row.get("author", ""),
            "date": row.get("date", ""),
            "topic": tid,
            "confidence": None,
            "sentiment": round(float(sentiment), 4),
            "corpus": "reuters",
            "label_method": "keyword",
            "snippet": clean_text(content, 230),
            "_entity_text": content,
            "url": row.get("url", ""),
        })
    return articles


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------
def word_pattern(name):
    """Case-insensitive whole-token match; avoids 'CIA' inside 'special'."""
    return re.compile(r"(?<![A-Za-z0-9])" + re.escape(name) + r"(?![A-Za-z0-9])",
                      re.IGNORECASE)


ROLE_WORDS = {"staff", "correspondent", "editor", "reporter", "news", "daily",
              "times", "press", "wire", "desk", "bureau"}


def extract_entities(articles, topics):
    topic_labels = {t["id"]: t["label"] for t in topics}

    publications = Counter(a["source"] for a in articles if a["source"])
    authors = Counter(a["author"] for a in articles if a["author"])

    org_names = list(ORG_GAZETTEER)
    for name, _count in publications.most_common(12):
        if name and name not in org_names:
            org_names.append(name)

    loc_names = list(LOC_GAZETTEER)
    person_names = []
    for name, count in authors.most_common(30):
        lowered = name.lower()
        if count < 2 or len(name) < 4:
            continue
        if any(role in lowered for role in ROLE_WORDS):
            continue
        person_names.append(name)

    entities = []

    def add(name, kind, matcher):
        matched = [a for a in articles if matcher(a)]
        if not matched:
            return
        buckets = Counter(sentiment_bucket(a["sentiment"]) for a in matched)
        entities.append({
            "name": name,
            "type": kind,
            "count": len(matched),
            "positive": buckets["positive"],
            "neutral": buckets["neutral"],
            "negative": buckets["negative"],
            "topics": Counter(a["topic"] for a in matched).most_common(3),
        })

    def content_text(article):
        return f"{article['title']} {article.get('_entity_text', article['snippet'])}"

    def normalized_name(value):
        normalized = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
        return normalized[4:] if normalized.startswith("the ") else normalized

    def is_article_metadata(article, name):
        normalized = normalized_name(name)
        return any(
            normalized_name(article.get(field, "")) == normalized
            for field in ("source", "author")
            if article.get(field)
        )

    def matches_content(article, name, pattern):
        return (not is_article_metadata(article, name)
                and bool(pattern.search(content_text(article))))

    for name in org_names:
        pattern = word_pattern(name)
        add(name, "ORG", lambda a, n=name, p=pattern: matches_content(a, n, p))
    for name in person_names:
        pattern = word_pattern(name)
        add(name, "PERSON", lambda a, n=name, p=pattern: matches_content(a, n, p))
    for name in loc_names:
        pattern = word_pattern(name)
        add(name, "LOC", lambda a, n=name, p=pattern: matches_content(a, n, p))

    dedup = {}
    for entity in entities:
        key = (entity["name"].lower(), entity["type"])
        if key not in dedup or entity["count"] > dedup[key]["count"]:
            dedup[key] = entity

    entities = sorted(dedup.values(), key=lambda e: -e["count"])[:24]

    for entity in entities:
        entity["topics"] = [{"id": tid, "label": topic_labels.get(tid, tid)}
                            for tid, _ in entity["topics"]]

    # Attach up to four known entity names to every article for inline NER tags.
    by_name = {e["name"]: e for e in entities}
    for article in articles:
        text = content_text(article)
        tags = []
        for name, entity in by_name.items():
            if (not is_article_metadata(article, name)
                    and word_pattern(name).search(text)):
                tags.append(name)
        article["entities"] = list(dict.fromkeys(tags))[:4]
    return entities


# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
def build_kpis(topics, months, timeline, entities, articles):
    total_articles = sum(t["volume"] for t in topics)
    weighted = sum(t["sentiment"] * t["volume"] for t in topics) / total_articles
    recent = months[-6:]
    prior = months[-12:-6]
    recent_total = sum(timeline[m]["total"] for m in recent)
    prior_total = sum(timeline[m]["total"] for m in prior)
    delta = ((recent_total - prior_total) / prior_total * 100
             if prior_total else 0.0)

    growth = {}
    for topic in topics:
        last = sum(timeline[m]["by_topic"].get(topic["id"], {}).get("count", 0)
                   for m in recent)
        before = sum(timeline[m]["by_topic"].get(topic["id"], {}).get("count", 0)
                     for m in prior)
        growth[topic["id"]] = ((last - before) / before * 100) if before else 0.0
    trending = max(topics, key=lambda t: growth[t["id"]])
    trending["growth"] = round(growth[trending["id"]], 1)

    top_entity = max(entities, key=lambda e: e["count"],
                     default={"name": "None", "type": "—", "count": 0})
    top_topic = max(topics, key=lambda t: t["volume"])

    sparkline = [timeline[m]["total"] for m in months[-12:]]
    sentiment_spark = [round(timeline[m]["sentiment"], 3) for m in months[-12:]]
    topic_spark = [sum(1 for t in topics
                       if timeline[m]["by_topic"].get(t["id"], {}).get("count", 0))
                   for m in months[-12:]]
    entity_spark = []
    for m in months[-12:]:
        entity_spark.append(sum(
            1 for a in articles
            if a["date"][:7] == m
            and top_entity["name"] in a.get("entities", [])))

    benchmark = load_json(SCALE_BENCHMARK)
    benchmark_rows_per_second = float(
        benchmark["throughput"]["end_to_end_rows_per_sec"])

    return {
        "total_articles": total_articles,
        "throughput": {
            "articles_per_hour": round(benchmark_rows_per_second * 3600),
            "source": "data/scale_benchmark.json · throughput.end_to_end_rows_per_sec",
        },
        "topic_count": len(topics),
        "net_sentiment": round(weighted, 3),
        "recent_total": recent_total,
        "delta_pct": round(delta, 1),
        "sparkline": sparkline,
        "sentiment_sparkline": sentiment_spark,
        "topic_sparkline": topic_spark,
        "entity_sparkline": entity_spark,
        "trending_topic": {
            "id": trending["id"],
            "label": trending["label"],
            "growth": trending["growth"],
        },
        "top_entity": {
            "name": top_entity["name"],
            "type": top_entity["type"],
            "count": top_entity["count"],
        },
        "top_topic": {"label": top_topic["label"], "volume": top_topic["volume"]},
        "embedded_articles": len(articles),
        "date_min": min(a["date"] for a in articles if a["date"]),
        "date_max": max(a["date"] for a in articles if a["date"]),
    }


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------
def build_payload():
    topics = build_topics()
    months, timeline = build_timeline(topics)
    articles = load_synth_articles(topics) + load_real_articles(topics)
    articles.sort(key=lambda a: a["date"], reverse=True)
    entities = extract_entities(articles, topics)
    kpis = build_kpis(topics, months, timeline, entities, articles)
    for article in articles:
        article.pop("_entity_text", None)

    sources = sorted({a["source"] for a in articles if a["source"]})
    for topic in topics:
        topic["share"] = round(topic["volume"] / kpis["total_articles"] * 100, 1)
        topic["bucket"] = sentiment_bucket(topic["sentiment"])

    payload = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "kpis": kpis,
        "topics": topics,
        "months": months,
        "timeline": timeline,
        "entities": entities,
        "articles": articles,
        "sources": sources,
        "provenance": {
            "corpora": [
                {
                    "name": "Synthetic benchmark",
                    "records": 6000,
                    "file": "data/pipeline_analytics_summary.json",
                },
                {
                    "name": "Reuters-21578 wire",
                    "records": 10712,
                    "file": "data/real_news/pipeline_analytics_summary.json",
                },
            ],
            "article_source": "data/hadoop_ecosystem/hbase/news_articles.json "
                              "+ sampled rows from the two raw CSV corpora",
            "notes": [
                "Topic volume, sentiment, confidence and monthly series are "
                "measured PySpark LDA outputs; no values are hand-typed.",
                "Reuters article-to-topic assignment is a learned-term keyword "
                "match for display only (the real corpus summary is aggregate).",
                "Named entities come from gazetteer matches in article titles "
                "and content; publication and byline fields are not counted.",
                "Throughput is a measured local end-to-end benchmark rate, not "
                "a live streaming rate.",
                "Reuters article sentiment is VADER compound; synthetic rows use "
                "the pipeline's stored sentiment polarity.",
            ],
        },
    }
    return payload


def render(payload):
    with open(TEMPLATE, "r", encoding="utf-8") as handle:
        template = handle.read()
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    data = data.replace("</", "<\\/")  # keep the JSON island safe
    html = template.replace("__PAYLOAD__", data)
    with open(OUTPUT, "w", encoding="utf-8") as handle:
        handle.write(html)
    return OUTPUT


def main():
    payload = build_payload()
    output = render(payload)
    size = os.path.getsize(output) / 1e6
    print(f"[OK] Wrote {os.path.relpath(output, PROJECT_ROOT)} "
          f"({size:.2f} MB, {len(payload['articles'])} articles, "
          f"{len(payload['topics'])} topics, {len(payload['entities'])} entities)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
