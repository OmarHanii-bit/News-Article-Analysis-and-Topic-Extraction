"""
Architecture Diagram Generator
==============================
Renders the Mermaid blueprint from PROJECT_REPORT.md §3.1 as a publication-ready
PNG using only Matplotlib (headless Agg backend — no graphviz / no image model).

Flow: Data sources -> Ingestion (Flume / Sqoop) -> HDFS -> YARN / ZooKeeper ->
MapReduce / Pig / Hive / Spark -> Parquet / HBase / MongoDB -> Streamlit / Jupyter
-> Insights.

Run:
    python src/generate_architecture_diagram.py
Output:
    presentation/architecture.png (300 DPI)
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")  # headless rendering
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

try:
    from src.viz_palette import TOPIC_PALETTE
except ImportError:  # pragma: no cover - direct script execution
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from viz_palette import TOPIC_PALETTE

OUTPUT = "presentation/architecture.png"
NAVY = TOPIC_PALETTE[2]
TEAL = TOPIC_PALETTE[0]
ORANGE = TOPIC_PALETTE[1]
GOLD = TOPIC_PALETTE[3]
SAGE = TOPIC_PALETTE[4]
PLUM = TOPIC_PALETTE[5]

# (tier label, accent, [(box title, box subtitle), ...])
TIERS = [
    ("1 · SOURCES", TEAL, [
        ("Synthetic Benchmark", "6,000 full-text articles\nKaggle 'All the News' schema"),
        ("Reuters-21578 Wire", "10,712 real full-text\ndocuments (NLTK)"),
        ("Live RSS Wire Feeds", "NYT, BBC, CBS, NPR\nRFC-822 live syndication"),
        ("Messy Web Feeds", "HTML tags · XML entities\n· tracking parameters"),
    ]),
    ("2 · INGESTION", ORANGE, [
        ("Apache Sqoop", "RDBMS ↔ HDFS\nparallel bulk transfer"),
        ("Apache Flume", "Spool source → memory\nchannel → HDFS sink"),
        ("Streaming Ingestor", "Live RSS fetcher &\nnormalizer daemon"),
        ("Schema Contract", "12-column StructType\nUTF-8 CSV enforcement"),
    ]),
    ("3 · HDFS STORAGE", SAGE, [
        ("Apache HDFS Core", "128 MB blocks · 3×\nreplication · rack-aware"),
        ("NameNode & DataNodes", "Active NameNode metadata\n· 3 cluster DataNodes"),
        ("HDFS Storage Integrity", "27 objects · zero loss\nlogical × 3 = physical"),
    ]),
    ("4 · RESOURCE / COORD", GOLD, [
        ("Apache YARN RM", "ResourceManager · cluster\ncapacity scheduler"),
        ("Apache ZooKeeper", "Leader election · failover\n& distributed consensus"),
        ("YARN NodeManagers", "Containerized execution\nworkers & AM lifecycle"),
    ]),
    ("5 · COMPUTE ENGINES", TEAL, [
        ("MapReduce", "Streaming mapper/reducer\nregex N-gram counting"),
        ("Apache Pig & Hive", "Pig Latin ETL filter\nHiveQL external schema"),
        ("PySpark Batch MLlib", "TF-IDF · Online LDA k=6\nVADER sentiment UDF"),
        ("PySpark Streaming", "Micro-batch streaming &\nvolatility alerts"),
    ]),
    ("6 · PERSISTENCE", PLUM, [
        ("Parquet Lakehouse", "Snappy · partitioned\nby year / month"),
        ("Apache HBase", "cf_meta / cf_nlp / cf_raw\n3 families · 16 regions"),
        ("MongoDB NoSQL", "BSON articles · compound\n+ text search indexes"),
    ]),
    ("7 · ANALYTICS / UI", ORANGE, [
        ("Matplotlib / Seaborn", "17 publication-quality\n300 DPI analytical charts"),
        ("Streamlit Command Center", "Executive dashboard +\n2 offline Plotly suites"),
        ("Jupyter / Colab", "29-cell reproducible\nend-to-end notebooks"),
    ]),
    ("8 · INSIGHTS", NAVY, [
        ("Risk Alerts", "Regulatory volume\n> mean + 2σ anomaly"),
        ("Editorial Assignments", "Topic velocity &\npeer share shifts > 10 pp"),
        ("Supervised Truth", "Reuters ground truth\n92.6% Acc / 0.84 Macro-F1"),
    ]),
]


def draw_box(ax, x, y, width, height, title, subtitle, accent):
    """Draw one rounded component box with a title and two-line subtitle."""
    patch = FancyBboxPatch(
        (x, y), width, height,
        boxstyle="round,pad=0.35,rounding_size=1.1",
        linewidth=1.3, edgecolor=accent, facecolor="white", zorder=3,
    )
    ax.add_patch(patch)
    ax.text(x + width / 2, y + height - 1.5, title, ha="center", va="center",
            fontsize=9.2, fontweight="bold", color=NAVY, zorder=4)
    ax.text(x + width / 2, y + height - 4.0, subtitle, ha="center", va="center",
            fontsize=7.6, color="#3C4B5A", linespacing=1.25, zorder=4)


def draw_tier(ax, y, label, accent, components):
    """Draw a background band, its tier label and all component boxes."""
    height = 9.6
    band = FancyBboxPatch(
        (2, y), 96, height,
        boxstyle="round,pad=0.4,rounding_size=1.5",
        linewidth=0.8, edgecolor="#D7E0E7", facecolor="#F7FAFC", zorder=1,
    )
    ax.add_patch(band)
    ax.text(3.8, y + height / 2, label, ha="center", va="center", rotation=90,
            fontsize=7.2, fontweight="bold", color=accent, zorder=2)

    n = len(components)
    box_w = 25 if n <= 3 else 19.2
    gap = (90 - n * box_w) / max(n - 1, 1)
    xs = [7 + i * (box_w + gap) for i in range(n)]
    centers = []
    for x, (title, subtitle) in zip(xs, components):
        draw_box(ax, x, y + 1.0, box_w, 7.6, title, subtitle, accent)
        centers.append(x + box_w / 2)
    return centers


def draw_arrow(ax, x, y_start, y_end, color=NAVY, style="-|>"):
    """Vertical arrow between two vertically stacked tiers."""
    arrow = FancyArrowPatch(
        (x, y_start), (x, y_end), arrowstyle=style, mutation_scale=13,
        linewidth=1.4, color=color, shrinkA=0, shrinkB=0, zorder=2,
    )
    ax.add_patch(arrow)


def build_diagram():
    fig, ax = plt.subplots(figsize=(16.5, 13.5))
    fig.patch.set_facecolor("white")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(50, 98.4, "News Article Analysis & Topic Extraction",
            ha="center", va="center", fontsize=19, fontweight="bold", color=NAVY)
    ax.text(50, 95.6, "Distributed Big Data Architecture — sources → ingestion → HDFS → YARN/ZooKeeper → "
                      "processing → persistence → UI → insights (PROJECT_REPORT.md §3.1)",
            ha="center", va="center", fontsize=9.4, color="#505F6E")

    tier_top = 91.5
    band_height = 9.6
    tier_step = 10.9
    all_centers = []
    for index, (label, accent, components) in enumerate(TIERS):
        y = tier_top - index * tier_step - band_height
        all_centers.append(draw_tier(ax, y, label, accent, components))

    # Connect consecutive tiers. Map several boxes to the nearest box in the
    # next tier so every stage visibly feeds the next one.
    for index, (upper, lower) in enumerate(zip(all_centers[:-1], all_centers[1:])):
        y_from = tier_top - index * tier_step - band_height
        y_to = tier_top - (index + 1) * tier_step
        for x in lower:
            src = min(upper, key=lambda candidate: abs(candidate - x))
            if abs(src - x) > 0.1:
                # Slight fan-out: draw a straight diagonal from the nearest
                # upper box centre to this lower box centre.
                elbow = FancyArrowPatch(
                    (src, y_from + 0.1), (x, y_to - 0.1),
                    arrowstyle="-|>", mutation_scale=11, linewidth=0.9,
                    color="#8BA3B5", zorder=2,
                )
                ax.add_patch(elbow)
            else:
                draw_arrow(ax, x, y_from + 0.2, y_to - 0.2)

    # ZooKeeper coordinates YARN (dashed lateral links inside tier 4).
    y_coord = tier_top - 3 * tier_step - band_height + 4.8
    # Left link: ZooKeeper (box 1, left edge 39.5) -> YARN RM (box 0, right edge 32.0)
    ax.annotate(
        "sync", xy=(32.2, y_coord), xytext=(39.3, y_coord),
        arrowprops=dict(arrowstyle="->", linestyle="--", color=GOLD, linewidth=1.2),
        va="center", ha="center", fontsize=7.2, color=GOLD,
    )
    # Right link: ZooKeeper (box 1, right edge 64.5) -> YARN NodeManagers (box 2, left edge 72.0)
    ax.annotate(
        "elect", xy=(71.8, y_coord), xytext=(64.7, y_coord),
        arrowprops=dict(arrowstyle="->", linestyle="--", color=GOLD, linewidth=1.2),
        va="center", ha="center", fontsize=7.2, color=GOLD,
    )

    ax.text(50, 1.4,
            "Verified stack: 9/9 Hadoop services executed · Oozie DAG SUCCEEDED · "
            "HDFS invariant: logical × 3 = physical · 17 × 300-DPI static artifacts",
            ha="center", va="center", fontsize=8.2, color="#505F6E")

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    fig.savefig(OUTPUT, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"[+] Saved architecture diagram: {OUTPUT} (300 DPI)")


if __name__ == "__main__":
    build_diagram()
