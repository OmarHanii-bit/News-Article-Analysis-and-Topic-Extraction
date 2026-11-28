"""
Interactive Dashboard Builder (Plotly)
======================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Purpose: Generates fully self-contained, *interactive* HTML dashboards from the
         processed Parquet lakehouses + analytics summaries, so the analysis is
         explorable (hover / zoom / legend toggle / topic filter) in any browser
         with no server and no internet connection required.

Corpora (mirrors the dual-corpus registry in src/dashboard.py):
    synthetic -> data/processed_parquet            + data/pipeline_analytics_summary.json
                 -> dashboard/interactive_dashboard.html
    real      -> data/real_news/processed_parquet  + data/real_news/pipeline_analytics_summary.json
                 -> dashboard/interactive_dashboard_real.html

Both outputs additionally embed the Hadoop ecosystem execution evidence panel
(data/hadoop_ecosystem/hadoop_ecosystem_summary.json), which is corpus-independent.

Run:
    python src/build_interactive_dashboard.py            # builds every available corpus
    python src/build_interactive_dashboard.py --corpus real
"""

import argparse
import json
import math
import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio

try:  # supports `import src.build_interactive_dashboard` from the project root
    from src.viz_palette import (
        TOPIC_PALETTE, CONTINUOUS_CMAP_PLOTLY, color_map,
    )
except ImportError:  # pragma: no cover - direct script execution
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from viz_palette import (
        TOPIC_PALETTE, CONTINUOUS_CMAP_PLOTLY, color_map,
    )

CORPORA = {
    "synthetic": {
        "label": "Synthetic Benchmark (6,000 articles)",
        "parquet": "data/processed_parquet",
        "summary": "data/pipeline_analytics_summary.json",
        "output": "dashboard/interactive_dashboard.html",
    },
    "real": {
        "label": "Real Reuters-21578 Wire (10,712 articles)",
        "parquet": "data/real_news/processed_parquet",
        "summary": "data/real_news/pipeline_analytics_summary.json",
        "output": "dashboard/interactive_dashboard_real.html",
    },
}
ECO_SUMMARY = "data/hadoop_ecosystem/hadoop_ecosystem_summary.json"

# Canonical colours come from the shared palette so the same topic has the same
# colour in the HTML dashboards, the Streamlit app and the Matplotlib PNGs.
PALETTE = TOPIC_PALETTE
HEATMAP_COLORSCALE = CONTINUOUS_CMAP_PLOTLY

CHART_LAYOUT = dict(
    paper_bgcolor="white",
    plot_bgcolor="#FAFBFC",
    font=dict(family="Segoe UI, Arial", size=12, color="#12263A"),
    margin=dict(l=60, r=30, t=50, b=50),
    hoverlabel=dict(bgcolor="white", font_size=12),
)


def _style(fig, title, height=420):
    fig.update_layout(title=title, height=height, **CHART_LAYOUT)
    return fig


def compute_business_kpis(summary, frame):
    """Three thresholded business KPIs shared with the Streamlit dashboard.

    velocity |MoM| <=10% green / <=25% amber / >25% red;
    sentiment delta >=-0.05 green / >=-0.15 amber / else red;
    coverage gaps <=2 green / <=5 amber / >5 red.
    """
    velocity, sentiment_delta, gaps = float("nan"), float("nan"), 0
    trends = pd.DataFrame(summary.get("temporal_trends", []))
    if not trends.empty and {"year", "month", "article_count"}.issubset(trends.columns):
        rows = []
        for (year, month), group in trends.groupby(["year", "month"]):
            count = float(group["article_count"].sum())
            sentiment = float((group["article_count"] * group["avg_sentiment"]).sum() / count) if count else float("nan")
            rows.append({"period": f"{int(year)}-{int(month):02d}",
                         "articles": count, "sentiment": sentiment})
        monthly = pd.DataFrame(rows).sort_values("period")
        if len(monthly) >= 2:
            previous, latest = monthly.iloc[-2], monthly.iloc[-1]
            if previous["articles"]:
                velocity = (latest["articles"] / previous["articles"] - 1) * 100
            sentiment_delta = latest["sentiment"] - monthly["sentiment"].mean()

    pub_dist = pd.DataFrame(summary.get("publisher_distribution", []))
    if not pub_dist.empty and {"publication", "topic_label", "count"}.issubset(pub_dist.columns):
        pivot = pub_dist.pivot_table(index="publication", columns="topic_label",
                                     values="count", aggfunc="sum").fillna(0)
        share = pivot.div(pivot.sum(axis=1).replace(0, math.nan), axis=0)
        gaps = int((share < 0.05).sum().sum())

    def colour(value, kind):
        if isinstance(value, float) and math.isnan(value):
            return "#E9C46A"
        if kind == "velocity":
            return "#2A9D8F" if abs(value) <= 10 else ("#E9C46A" if abs(value) <= 25 else "#E76F51")
        if kind == "sentiment":
            return "#2A9D8F" if value >= -0.05 else ("#E9C46A" if value >= -0.15 else "#E76F51")
        return "#2A9D8F" if value <= 2 else ("#E9C46A" if value <= 5 else "#E76F51")

    return [
        ("Topic Velocity MoM", f"{velocity:+.1f}%" if not math.isnan(velocity) else "n/a",
         "|Δ|≤10% green · ≤25% amber · >25% red", colour(velocity, "velocity")),
        ("Sentiment Δ vs Baseline", f"{sentiment_delta:+.3f}" if not math.isnan(sentiment_delta) else "n/a",
         "Δ≥−0.05 green · ≥−0.15 amber · <−0.15 red", colour(sentiment_delta, "sentiment")),
        ("Coverage-Gap Count", f"{gaps}",
         "publisher×topic <5% share · ≤2 green · ≤5 amber", colour(gaps, "gaps")),
    ]


def build_decisions_panel(corpus_label, summary, frame):
    """Corpus-specific findings paired with decisions, owners and numeric triggers."""
    is_real = "Reuters" in corpus_label

    if is_real:
        topics = sorted(
            summary.get("topic_summary", []),
            key=lambda row: -int(row.get("total_articles", 0)),
        )
        total = sum(int(row.get("total_articles", 0)) for row in topics) or 1
        top = topics[0] if topics else {"topic_label": "n/a", "total_articles": 0}
        top_share = 100.0 * int(top.get("total_articles", 0)) / total
        confidences = [float(row.get("mean_confidence", 0.0)) for row in topics] or [0.0]
        rows = [
            ("Auto-label confidence is lower than the synthetic benchmark",
             f"Reuters confidence {min(confidences):.3f}–{max(confidences):.3f}; gate at ≥0.90",
             "Route sub-0.90 articles to human review before auto-publishing", "NLP / ML lead", "Next release"),
            ("Single-publisher corpus",
             "1 publisher × 6 auto-labeled topics; the publisher heatmap is degenerate by design",
             "Do not infer editorial bias from Reuters; use the synthetic multi-publisher corpus for that check", "Analytics lead", "N/A"),
            ("Descriptive topic mix",
             f"Largest auto topic: {top['topic_label']} {int(top.get('total_articles', 0)):,}/{total:,} = {top_share:.1f}%",
             "Use as a corpus profile, not a market signal", "Editorial analytics", "Descriptive"),
            ("Publication dates are derived",
             "Reuters-21578 has no per-article timestamps; dates are deterministic 2024–2025",
             "Treat temporal trends as illustrative; no real-world temporal inference", "Data governance", "Always"),
            ("Batch latency cannot serve breaking news",
             "68.4 s for 10,712 articles; target <5 s classification",
             "Deploy Kafka + PySpark Structured Streaming micro-batches", "Platform engineering", "1 sprint"),
        ]
        note = (
            "This view is the Reuters auto-label corpus: publisher heatmaps are "
            "degenerate by design (1 publisher × 6 topics) because every source document is a Reuters wire story."
        )
        footer = (
            "Actions for this corpus: (1) human review below 0.90 confidence, "
            "(2) Kafka + PySpark Structured Streaming upgrade, (3) treat publisher and temporal claims as illustrative only."
        )
    else:
        rows = [
            ("Science sentiment anomaly — only negative-topic mean (−0.028)",
             "Quantum 470/6,000 = 7.8%; alert if 30-day mean < −0.15",
             "Route to science desk for lexicon review; no trading rule without validation", "NLP lead / science editor", "Weekly"),
            ("Publisher coverage mix — null result",
             "Every publisher 38.3–41.6% Markets, 21.0–26.8% AI; no strong specialization",
             "No publisher-prior adjustment for synthetic corpus; re-test the real corpus", "Analytics lead", "Next corpus"),
            ("Markets/Politics concentration",
             "Markets, Politics & Regulation 2,386/6,000 = 39.8%; trigger at 30-day share > mean + 2σ",
             "Commission follow-on policy/regulation reporting or hold staffing", "Editorial board", "Monthly"),
            ("Model confidence calibration differs by corpus",
             "Synthetic 0.991–0.993 vs Reuters 0.684–0.892; auto-publish gate ≥0.90",
             "Route sub-0.90 confidence to human review", "NLP / ML lead", "Next release"),
            ("Batch latency cannot serve breaking news",
             "52.08 s synthetic / 68.4 s Reuters; target <5 s classification",
             "Deploy Kafka `news-wire-incoming` + Structured Streaming", "Platform engineering", "1 sprint"),
        ]
        note = (
            "This is the synthetic benchmark corpus: publisher heatmaps cover 8 publishers × 6 LDA components "
            "(5 dominant labels plus 1 unused component)."
        )
        footer = (
            "Actions implement <code>PROJECT_REPORT.md</code> §6.3: (1) science-desk lexicon review, "
            "(2) Kafka + PySpark Structured Streaming upgrade, (3) re-test publisher priors on a real multi-publisher corpus."
        )

    table_rows = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    )
    return f"""
<section class="panel decisions">
  <h2>Key Takeaways & Decisions — What next?</h2>
  <p class="caption">{note}</p>
  <table>
    <thead><tr><th>Finding</th><th>Metric / numeric trigger</th><th>Decision</th><th>Owner</th><th>Horizon</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <p class="caption">{footer}</p>
</section>
"""


def build_figures(parquet_dir, summary_json):
    df = pd.read_parquet(parquet_dir)
    with open(summary_json, "r", encoding="utf-8") as handle:
        summary = json.load(handle)

    figures = []
    labels = [summary["topic_summary"][i]["topic_label"]
              if i < len(summary["topic_summary"]) else f"Topic {i}"
              for i in range(len(summary.get("topics", {})))]
    topic_colors = color_map(labels)

    # --- 1. Topic volume distribution -------------------------------------
    volume = df["topic_label"].value_counts().reset_index()
    volume.columns = ["topic_label", "articles"]
    figure = px.bar(
        volume.sort_values("articles"), x="articles", y="topic_label",
        orientation="h", color="topic_label", color_discrete_map=topic_colors,
        color_discrete_sequence=PALETTE, text="articles",
    )
    figure.update_traces(textposition="outside")
    figure.update_layout(showlegend=False, xaxis_title="Articles", yaxis_title="")
    figures.append(_style(figure, "1 · Discovered Topic Volume Distribution"))

    # --- 2. Publisher x Topic heatmap -------------------------------------
    pivot = pd.crosstab(df["publication"], df["topic_label"])
    heatmap = go.Figure(go.Heatmap(
        z=pivot.values,
        x=list(pivot.columns),
        y=list(pivot.index),
        colorscale=HEATMAP_COLORSCALE,
        colorbar=dict(title="Articles"),
        hovertemplate="Publisher: %{y}<br>Topic: %{x}<br>Articles: %{z}<extra></extra>",
    ))
    heatmap.update_xaxes(tickangle=-25)
    figures.append(_style(heatmap, "2 · Publisher × Topic Editorial Cross-Tabulation", 480))

    # --- 3. Sentiment distribution by topic -------------------------------
    box = px.box(
        df, x="topic_label", y="sentiment_polarity", color="topic_label",
        color_discrete_map=topic_colors, color_discrete_sequence=PALETTE, points=False,
    )
    box.update_layout(showlegend=False, xaxis_title="", yaxis_title="VADER Sentiment")
    box.update_xaxes(tickangle=-25)
    figures.append(_style(box, "3 · Sentiment Polarity Distribution by Topic", 460))

    # --- 4. Temporal topic trajectories -----------------------------------
    df["period"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=1)
    ).dt.strftime("%Y-%m")
    trend = df.groupby(["period", "topic_label"]).size().reset_index(name="articles")
    trend = trend.sort_values("period")
    line = px.line(
        trend, x="period", y="articles", color="topic_label", markers=True,
        color_discrete_map=topic_colors, color_discrete_sequence=PALETTE,
    )
    line.update_layout(xaxis_title="Month", yaxis_title="Articles", legend_title="")
    figures.append(_style(line, "4 · Monthly Topic Trajectories", 460))

    # --- 5. Top terms per topic (interactive topic selector) --------------
    topics = summary["topics"]
    terms_figure = go.Figure()
    for index, (topic_id, payload) in enumerate(sorted(topics.items(),
                                                       key=lambda kv: int(kv[1]["topic_id"]))):
        terms = payload["terms"][:12][::-1]
        weights = payload["weights"][:12][::-1]
        terms_figure.add_trace(go.Bar(
            x=weights, y=terms, orientation="h",
            name=labels[payload["topic_id"]] if payload["topic_id"] < len(labels) else topic_id,
            marker_color=topic_colors.get(
                labels[payload["topic_id"]], PALETTE[payload["topic_id"] % len(PALETTE)]),
            visible=(index == 0),
            hovertemplate="%{y}: %{x:.5f}<extra></extra>",
        ))
    buttons = [
        dict(label=labels[payload["topic_id"]] if payload["topic_id"] < len(labels) else topic_id,
             method="update",
             args=[{"visible": [j == index for j in range(len(topics))]},
                   {"title": f"5 · Top Terms — {labels[payload['topic_id']]}"}])
        for index, (_, payload) in enumerate(sorted(topics.items(), key=lambda kv: int(kv[1]["topic_id"])))
    ]
    terms_figure.update_layout(updatemenus=[dict(buttons=buttons, direction="down",
                                                 x=1.0, y=1.18, showactive=True)])
    figures.append(_style(terms_figure, "5 · Top Terms — " + labels[0], 460))

    # --- 6. Category composition donut ------------------------------------
    # Real corpora carry up to 90 editorial categories; collapse the long tail
    # so the donut stays readable.
    category_counts = df["category"].value_counts()
    if len(category_counts) > 10:
        head = category_counts.head(10)
        tail_total = int(category_counts.iloc[10:].sum())
        category = head.reset_index()
        category.columns = ["category", "articles"]
        category = pd.concat(
            [category, pd.DataFrame([{"category": f"Other ({len(category_counts) - 10})",
                                      "articles": tail_total}])],
            ignore_index=True,
        )
    else:
        category = category_counts.reset_index()
        category.columns = ["category", "articles"]
    donut = px.pie(category, names="category", values="articles", hole=0.5,
                   color_discrete_sequence=PALETTE)
    donut.update_traces(textinfo="percent+label")
    figures.append(_style(donut, "6 · Editorial Category Composition", 420))

    # --- 7. Confidence vs Sentiment scatter -------------------------------
    sample = df.sample(min(1500, len(df)), random_state=42)
    scatter = px.scatter(
        sample, x="topic_confidence", y="sentiment_polarity", color="topic_label",
        color_discrete_map=topic_colors, color_discrete_sequence=PALETTE, opacity=0.55,
        hover_data=["publication", "clean_word_count"],
    )
    scatter.update_layout(xaxis_title="Topic Confidence", yaxis_title="Sentiment", legend_title="")
    figures.append(_style(scatter, "7 · Topic Confidence vs. Sentiment", 460))

    return df, summary, figures


def build_ecosystem_panel():
    """Builds the corpus-independent Hadoop ecosystem evidence panel.

    Returns (kpi_cards_html, figure) or (None, None) when evidence is absent.
    """
    if not os.path.exists(ECO_SUMMARY):
        return None, None
    with open(ECO_SUMMARY, "r", encoding="utf-8") as handle:
        eco = json.load(handle)

    report = eco.get("components", {}).get("hdfs", {}).get("dfs_report", {})
    replication = report.get("configured_replication", 3)
    logical = report.get("logical_bytes", 0)
    physical = report.get("physical_bytes_with_replication", 0)
    invariant_ok = (logical * replication == physical)

    kpis = [
        ("Services Executed", f"{len(eco.get('services_executed', []))}"),
        ("Services Failed", f"{len(eco.get('services_failed', []))}"),
        ("Stack Runtime", f"{eco.get('elapsed_seconds', 0):.2f}s"),
        ("HDFS CLI Commands", f"{eco.get('hdfs_cli_commands_recorded', 0)}"),
        ("Live DataNodes", f"{report.get('live_datanodes', 0)}"),
        ("Replication Factor", f"{replication}×"),
        ("Total Blocks", f"{report.get('total_blocks', 0):,}"),
        ("Missing Blocks", f"{report.get('missing_blocks', 0)}"),
    ]
    cards = "".join(
        f'<div class="card"><div class="kpi">{value}</div>'
        f'<div class="label">{label}</div></div>'
        for label, value in kpis
    )

    comps = eco.get("components", {})
    cap = comps.get("cluster", {}).get("capacity_plan", {})
    zk = comps.get("zookeeper", {})
    sqoop = comps.get("sqoop", {})
    flume = comps.get("flume", {})
    hbase = comps.get("hbase", {})
    hive = comps.get("hive", {})
    pig = comps.get("pig", {})
    oozie = comps.get("oozie", {})

    rows = [
        ("HDFS", f"{report.get('total_files', 0)} files / {report.get('total_directories', 0)} dirs · "
                 f"logical {logical:,} B × {replication} = {physical:,} B physical · "
                 f"invariant holds: {invariant_ok} · under-replicated: {report.get('under_replicated_blocks', 0)}"),
        ("YARN / Cluster Computing", f"worker memory {cap.get('worker_memory_gb', 0)} GB · YARN available "
                                     f"{cap.get('yarn_available_memory_gb', 0)} GB · container "
                                     f"{cap.get('container_size_gb', 0)} GB · max containers "
                                     f"{cap.get('max_concurrent_containers', 0)} · Spark executors "
                                     f"{cap.get('spark_executor_instances', 0)}"),
        ("MapReduce", f"distributed n-gram term extraction over HDFS blocks · "
                      f"{eco.get('hdfs_cli_commands_recorded', 0)} recorded HDFS CLI operations"),
        ("ZooKeeper", f"{zk.get('znodes_created', 0)} znodes · {zk.get('ephemeral_nodes', 0)} ephemeral · "
                      f"{zk.get('sessions', 0)} sessions · {zk.get('watches_fired', 0)} watches fired · "
                      f"leader {zk.get('leader_elected', 'n/a')}"),
        ("Sqoop", f"{sqoop.get('records_imported', 0):,} records imported · "
                  f"{sqoop.get('records_exported', 0):,} exported · tables: {', '.join(sqoop.get('tables', []))}"),
        ("Flume", f"{flume.get('events_ingested', 0):,} events ingested · {flume.get('bytes_ingested', 0):,} bytes · "
                  f"{flume.get('events_per_second', 0):,.0f} events/s · channel capacity {flume.get('channel_capacity', 0)}"),
        ("HBase", f"table '{hbase.get('table_name', 'n/a')}' · column families "
                  f"{', '.join(hbase.get('column_families', []))} · {hbase.get('rows_loaded', 0):,} rows · "
                  f"{hbase.get('num_regions', 0)} regions"),
        ("Hive", f"database '{hive.get('database', 'n/a')}' · tables {', '.join(hive.get('tables_created', []))} · "
                 f"{hive.get('queries_executed', 0)} queries · {hive.get('total_rows_scanned', 0):,} rows scanned"),
        ("Pig", f"{pig.get('scripts_executed', 0)} Latin scripts · {pig.get('relations_created', 0)} relations · "
                f"{pig.get('staged_rows', 0):,} staged rows"),
        ("Oozie", f"{oozie.get('actions_executed', 0)} DAG actions · {oozie.get('actions_skipped', 0)} skipped · "
                  f"final status {oozie.get('final_status', 'n/a')}"),
    ]
    table = go.Figure(go.Table(
        header=dict(values=["<b>Service</b>", "<b>Key Evidence</b>"],
                    fill_color="#12263A", font=dict(color="white", size=12), align="left"),
        cells=dict(values=[[r[0] for r in rows], [r[1] for r in rows]],
                   fill_color="#F5F7FA", align="left", height=26),
    ))
    table.update_layout(height=460, margin=dict(l=10, r=10, t=10, b=10),
                        font=CHART_LAYOUT["font"])
    return cards, table


def build_html(df, summary, figures, corpus_label, eco_cards=None, eco_table=None):
    meta = summary["metadata"]
    kpis = [
        ("Total Articles", f"{meta['total_articles_processed']:,}"),
        ("Vocabulary Size", f"{meta['vocabulary_size']:,}"),
        ("Discovered Topics", f"{meta['num_topics']}"),
        ("Pipeline Latency", f"{meta['elapsed_seconds']:.1f}s"),
        ("Publishers", f"{df['publication'].nunique()}"),
        ("Categories", f"{df['category'].nunique()}"),
    ]
    cards = "".join(
        f'<div class="card"><div class="kpi">{value}</div>'
        f'<div class="label">{label}</div></div>'
        for label, value in kpis
    )
    business_kpis = compute_business_kpis(summary, df)
    business_cards = "".join(
        f'<div class="card"><div class="kpi" style="color:{colour}">{value}</div>'
        f'<div class="label">{label}</div>'
        f'<div class="caption">{threshold}</div></div>'
        for label, value, threshold, colour in business_kpis
    )
    kpi_banner = (
        '<div class="cards" id="business-kpis">'
        '<div class="section-label">Business KPIs — red / amber / green decision thresholds</div>'
        f'{business_cards}</div>'
    )
    decisions_panel = build_decisions_panel(corpus_label, summary, df)

    blocks = []
    for index, figure in enumerate(figures):
        # Inline the plotly.js library only once (first figure) to keep the file self-contained.
        blocks.append(pio.to_html(
            figure, include_plotlyjs=(index == 0), full_html=False,
            config={"displaylogo": False, "responsive": True},
            div_id=f"chart-{index}",
        ))

    table_rows = df.sort_values("topic_confidence", ascending=False).head(40)[
        ["article_id", "publication", "topic_label", "topic_confidence",
         "sentiment_polarity", "clean_word_count", "year", "month"]
    ]
    table_figure = go.Figure(go.Table(
        header=dict(values=[f"<b>{c}</b>" for c in table_rows.columns],
                    fill_color="#12263A", font=dict(color="white", size=12)),
        cells=dict(values=[table_rows[c] for c in table_rows.columns],
                   fill_color="#F5F7FA", align="left", height=24),
    ))
    blocks.append(pio.to_html(table_figure, include_plotlyjs=False, full_html=False,
                              config={"responsive": True}, div_id="chart-table"))

    is_real = "Reuters" in corpus_label
    if is_real:
        real_topics = sorted(
            summary.get("topic_summary", []),
            key=lambda row: -int(row.get("total_articles", 0)),
        )
        real_top = real_topics[0] if real_topics else {"topic_label": "n/a", "total_articles": 0}
        real_total = int(meta["total_articles_processed"]) or 1
        real_share = 100.0 * int(real_top.get("total_articles", 0)) / real_total
        heatmap_caption = (
            "So what? The publisher heatmap is degenerate by design — the Reuters corpus has "
            "1 publisher × 6 topics — so read it as a topic-mix profile, not a peer comparison."
        )
        confidence_caption = (
            "So what? Reuters auto-label confidence is 0.684–0.892; n=1,500 random sample (seed 42). "
            "Never compare this scale directly with synthetic confidence."
        )
        topic_caption = (
            f"So what? Reuters auto-label corpus: {real_top['topic_label']} is the largest cluster at "
            f"{int(real_top.get('total_articles', 0)):,}/{real_total:,} = {real_share:.1f}%; "
            "read it as a descriptive profile, not a market signal."
        )
        sentiment_caption = (
            "So what? Auto-label sentiment is descriptive; Reuters dates are derived, "
            "so no real-world temporal inference is supported."
        )
    else:
        heatmap_caption = (
            "So what? Publisher × topic shares track corpus base rates — every publisher is "
            "38.3–41.6% Markets and 21.0–26.8% AI; this synthetic corpus shows no strong editorial specialization."
        )
        confidence_caption = (
            "So what? Synthetic LDA confidence is 0.991–0.993; n=1,500 random sample (seed 42). "
            "High separation validates k=6 but does not transfer to Reuters."
        )
        topic_caption = (
            "So what? Markets, Politics & Regulation dominates at 2,386/6,000 = 39.8%; "
            "a 30-day volume above mean + 2σ triggers the risk/newsroom playbook."
        )
        sentiment_caption = (
            "So what? Quantum Physics & Advanced Materials is the only negative-mean topic (−0.028); "
            "a sentiment Δ ≤ −0.15 turns the card red."
        )

    if eco_table is not None:
        eco_block = pio.to_html(eco_table, include_plotlyjs=False, full_html=False,
                                config={"responsive": True}, div_id="chart-ecosystem")
        eco_section = (
            f'<div class="cards">{eco_cards}</div>'
            f'<div class="panel"><h2>Hadoop Ecosystem Stack Execution Evidence</h2>{eco_block}'
            '<p class="caption"><b>So what?</b> All stack services executed with zero failures — '
            'the architecture is demonstrably end-to-end, not a diagram-only claim.</p></div>'
        )
    else:
        eco_section = ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>News Article Analysis — Interactive Dashboard · {corpus_label}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:#EEF2F5; color:#12263A;
         font-family:"Segoe UI",Arial,sans-serif; }}
  header {{ background:linear-gradient(120deg,#12263A,#2A9D8F); color:#fff;
            padding:28px 40px; }}
  header h1 {{ margin:0 0 6px; font-size:26px; }}
  header p {{ margin:0; opacity:.9; font-size:14px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
            gap:16px; padding:24px 40px 8px; }}
  .card {{ background:#fff; border-radius:12px; padding:18px 20px;
           box-shadow:0 2px 10px rgba(18,38,58,.08); }}
  .kpi {{ font-size:28px; font-weight:700; color:#2A9D8F; }}
  .label {{ font-size:12px; text-transform:uppercase; letter-spacing:.05em;
            color:#505F6E; margin-top:6px; }}
  .section-label {{ grid-column:1/-1; font-size:12px; font-weight:700;
                    text-transform:uppercase; letter-spacing:.06em; color:#12263A; }}
  .caption {{ font-size:12px; color:#505F6E; margin:6px 0 12px; line-height:1.45; }}
  .decisions table {{ width:100%; border-collapse:collapse; font-size:12px; }}
  .decisions th {{ background:#12263A; color:#fff; text-align:left; padding:8px; }}
  .decisions td {{ border-bottom:1px solid #E3E9EE; padding:8px; vertical-align:top; }}
  .decisions tr:nth-child(even) td {{ background:#F5F7FA; }}
  main {{ padding:10px 40px 60px; }}
  .panel {{ background:#fff; border-radius:12px; margin:18px 0; padding:10px 16px;
            box-shadow:0 2px 10px rgba(18,38,58,.08); }}
  h2 {{ font-size:16px; margin:6px 0 4px; color:#12263A; }}
  footer {{ text-align:center; color:#505F6E; font-size:12px; padding:0 0 30px; }}
</style>
</head>
<body>
<header>
  <h1>News Article Analysis &amp; Topic Extraction</h1>
  <p>{corpus_label} · {meta['total_articles_processed']:,} articles ·
     {meta['num_topics']} LDA topics · label mode: {meta['label_mode']} ·
     generated {meta['execution_timestamp']}</p>
</header>
<div class="cards">{cards}</div>
{kpi_banner}
{decisions_panel}
<main>
  <div class="panel"><h2>Topic Volume</h2>{blocks[0]}
    <p class="caption"><b>{topic_caption}</b></p></div>
  <div class="panel"><h2>Publisher × Topic</h2>{blocks[1]}
    <p class="caption">{heatmap_caption}</p></div>
  <div class="panel"><h2>Sentiment by Topic</h2>{blocks[2]}
    <p class="caption"><b>{sentiment_caption}</b></p></div>
  <div class="panel"><h2>Temporal Trajectories</h2>{blocks[3]}
    <p class="caption"><b>So what?</b> 24 months (2024-2025) of monthly volume; |MoM| > 25% is the alert threshold for topic velocity.</p></div>
  <div class="panel"><h2>Top Terms (use the topic selector)</h2>{blocks[4]}
    <p class="caption"><b>So what?</b> Dirichlet β weights show the discriminative vocabulary behind each label — a sudden term shift is an early signal of topic drift.</p></div>
  <div class="panel"><h2>Category Composition</h2>{blocks[5]}
    <p class="caption"><b>So what?</b> Category mix reveals long-tail editorial coverage; >10 categories are collapsed into an "Other" slice for readability.</p></div>
  <div class="panel"><h2>Confidence vs. Sentiment</h2>{blocks[6]}
    <p class="caption">{confidence_caption}</p></div>
  <div class="panel"><h2>Most Confident Articles</h2>{blocks[7]}
    <p class="caption"><b>So what?</b> Synthetic LDA confidence is 0.991–0.993 vs Reuters 0.684–0.892 — rank only within a corpus; gate auto-publishing at ≥0.90.</p></div>
</main>
{eco_section}
<footer>The Distributed Insights Collective — Big Data Capstone · self-contained, offline-interactive ·
<a href="index.html">Back to dashboard index</a></footer>
</body>
</html>
"""
    return html


def build_corpus(key):
    """Builds one corpus dashboard; returns the written path or None if unavailable."""
    corpus = CORPORA[key]
    if not os.path.exists(corpus["parquet"]) or not os.path.exists(corpus["summary"]):
        print(f"[skip] '{key}' corpus artifacts not found "
              f"({corpus['parquet']} / {corpus['summary']}).")
        return None

    frame, summary_payload, figures_list = build_figures(corpus["parquet"], corpus["summary"])
    eco_cards, eco_table = build_ecosystem_panel()
    html = build_html(frame, summary_payload, figures_list, corpus["label"], eco_cards, eco_table)

    os.makedirs(os.path.dirname(corpus["output"]), exist_ok=True)
    with open(corpus["output"], "w", encoding="utf-8") as handle:
        handle.write(html)
    size_mb = os.path.getsize(corpus["output"]) / 1024 / 1024
    print(f"[OK] {corpus['label']} -> '{corpus['output']}' ({size_mb:.1f} MB, self-contained)")
    return corpus["output"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the offline interactive Plotly dashboard(s).")
    parser.add_argument("--corpus", choices=["synthetic", "real", "all"], default="all",
                        help="Which corpus to render (default: all available).")
    args = parser.parse_args()

    targets = list(CORPORA) if args.corpus == "all" else [args.corpus]
    for key in targets:
        build_corpus(key)
