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
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio

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

PALETTE = [
    "#2A9D8F", "#E76F51", "#264653", "#E9C46A",
    "#8AB17D", "#6D597A", "#B56576", "#457B9D",
]

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


def build_figures(parquet_dir, summary_json):
    df = pd.read_parquet(parquet_dir)
    with open(summary_json, "r", encoding="utf-8") as handle:
        summary = json.load(handle)

    figures = []

    # --- 1. Topic volume distribution -------------------------------------
    volume = df["topic_label"].value_counts().reset_index()
    volume.columns = ["topic_label", "articles"]
    figure = px.bar(
        volume.sort_values("articles"), x="articles", y="topic_label",
        orientation="h", color="topic_label", color_discrete_sequence=PALETTE,
        text="articles",
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
        colorscale="Tealgrn",
        colorbar=dict(title="Articles"),
        hovertemplate="Publisher: %{y}<br>Topic: %{x}<br>Articles: %{z}<extra></extra>",
    ))
    heatmap.update_xaxes(tickangle=-25)
    figures.append(_style(heatmap, "2 · Publisher × Topic Editorial Cross-Tabulation", 480))

    # --- 3. Sentiment distribution by topic -------------------------------
    box = px.box(
        df, x="topic_label", y="sentiment_polarity", color="topic_label",
        color_discrete_sequence=PALETTE, points=False,
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
        color_discrete_sequence=PALETTE,
    )
    line.update_layout(xaxis_title="Month", yaxis_title="Articles", legend_title="")
    figures.append(_style(line, "4 · Monthly Topic Trajectories", 460))

    # --- 5. Top terms per topic (interactive topic selector) --------------
    topics = summary["topics"]
    labels = [summary["topic_summary"][i]["topic_label"]
              if i < len(summary["topic_summary"]) else f"Topic {i}"
              for i in range(len(topics))]
    terms_figure = go.Figure()
    for index, (topic_id, payload) in enumerate(sorted(topics.items(),
                                                       key=lambda kv: int(kv[1]["topic_id"]))):
        terms = payload["terms"][:12][::-1]
        weights = payload["weights"][:12][::-1]
        terms_figure.add_trace(go.Bar(
            x=weights, y=terms, orientation="h",
            name=labels[payload["topic_id"]] if payload["topic_id"] < len(labels) else topic_id,
            marker_color=PALETTE[payload["topic_id"] % len(PALETTE)],
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
        color_discrete_sequence=PALETTE, opacity=0.55,
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

    if eco_table is not None:
        eco_block = pio.to_html(eco_table, include_plotlyjs=False, full_html=False,
                                config={"responsive": True}, div_id="chart-ecosystem")
        eco_section = (
            f'<div class="cards">{eco_cards}</div>'
            f'<div class="panel"><h2>Hadoop Ecosystem Stack Execution Evidence</h2>{eco_block}</div>'
        )
    else:
        eco_section = ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>News Article Analysis — Interactive Dashboard</title>
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
<main>
  <div class="panel"><h2>Topic Volume</h2>{blocks[0]}</div>
  <div class="panel"><h2>Publisher × Topic</h2>{blocks[1]}</div>
  <div class="panel"><h2>Sentiment by Topic</h2>{blocks[2]}</div>
  <div class="panel"><h2>Temporal Trajectories</h2>{blocks[3]}</div>
  <div class="panel"><h2>Top Terms (use the topic selector)</h2>{blocks[4]}</div>
  <div class="panel"><h2>Category Composition</h2>{blocks[5]}</div>
  <div class="panel"><h2>Confidence vs. Sentiment</h2>{blocks[6]}</div>
  <div class="panel"><h2>Most Confident Articles</h2>{blocks[7]}</div>
</main>
{eco_section}
<footer>The Distributed Insights Collective — Big Data Capstone · self-contained, offline-interactive</footer>
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
