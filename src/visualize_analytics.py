"""
Big Data Visualization & Analytics Engine
=========================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Authors: Big Data Engineering Team (Member 6: Visualization & UI Lead)

Description:
------------
Generates publication-quality charts and visual analytics from the PySpark
and MapReduce pipeline outputs. Saves static high-resolution graphics (PNG)
to the 'visualizations/' directory and provides summary metrics for executive reporting.

Visualizations Generated:
-------------------------
1. `topic_volume_distribution.png`: Bar chart of article volume per extracted topic.
2. `lda_topic_top_terms.png`: 6-panel horizontal bar chart of top keyword Dirichlet weights.
3. `publisher_topic_heatmap.png`: Cross-tabulation heatmap of news publishers vs topic coverage.
4. `temporal_topic_trends.png`: Multi-line time series of monthly topic trajectory.
5. `topic_sentiment_distribution.png`: Boxplot / Violin plot of sentiment polarity per topic.
6. `topic_wordclouds_grid.png`: Visual multi-panel word clouds for extracted topics.
"""

import os
import sys
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

# Configure Matplotlib styling for professional publication standards
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['figure.dpi'] = 300

def load_data(
    summary_json_path="data/pipeline_analytics_summary.json",
    parquet_dir="data/processed_parquet"
):
    """Loads analytical summaries and Parquet records."""
    if not os.path.exists(summary_json_path):
        print(f"[!] File not found: {summary_json_path}. Please execute spark_pipeline.py first.")
        return None, None

    with open(summary_json_path, "r", encoding="utf-8") as f:
        summary_data = json.load(f)

    df_parquet = None
    if os.path.exists(parquet_dir):
        try:
            df_parquet = pd.read_parquet(parquet_dir)
        except Exception as e:
            print(f"[!] Parquet read warning: {e}")

    return summary_data, df_parquet

def plot_topic_volume(summary_data, output_dir="visualizations"):
    """Plot 1: Bar chart of article volume per topic."""
    topics_summary = pd.DataFrame(summary_data.get("topic_summary", []))
    if topics_summary.empty:
        return

    plt.figure(figsize=(10, 5.5))
    palette = sns.color_palette("mako", len(topics_summary))
    ax = sns.barplot(
        x="total_articles",
        y="topic_label",
        data=topics_summary,
        palette=palette,
        hue="topic_label",
        legend=False
    )
    
    # Annotate bar totals
    for p in ax.patches:
        width = p.get_width()
        ax.annotate(
            f"{int(width):,} ({width/topics_summary['total_articles'].sum():.1%})",
            (width + 25, p.get_y() + p.get_height() / 2),
            ha='left', va='center', fontsize=9, weight='bold', color='#2c3e50'
        )

    plt.title("Extracted Topic Volume & Document Distribution (PySpark LDA)", fontsize=13, weight='bold', pad=15)
    plt.xlabel("Total Ingested Articles", fontsize=11, labelpad=10)
    plt.ylabel("Extracted Latent Topic", fontsize=11)
    plt.xlim(0, topics_summary["total_articles"].max() * 1.25)
    plt.tight_layout()
    
    save_path = os.path.join(output_dir, "topic_volume_distribution.png")
    plt.savefig(save_path)
    plt.close()
    print(f"[+] Saved: {save_path}")

def plot_lda_top_terms(summary_data, output_dir="visualizations"):
    """Plot 2: Multi-panel horizontal bar chart of top keyword Dirichlet weights."""
    topics = summary_data.get("topics", {})
    if not topics:
        return

    num_topics = len(topics)
    cols = 3
    rows = (num_topics + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(16, 5 * rows), sharex=False)
    axes = axes.flatten()

    colors = ["#2b5c8f", "#2a9d8f", "#e76f51", "#7209b7", "#3a86ff", "#f4a261"]

    for idx, (tkey, tdata) in enumerate(topics.items()):
        ax = axes[idx]
        terms = tdata["terms"][:10][::-1]
        weights = tdata["weights"][:10][::-1]
        
        ax.barh(terms, weights, color=colors[idx % len(colors)], alpha=0.85, edgecolor='black', linewidth=0.5)
        ax.set_title(f"Topic {tdata['topic_id']}: {tdata.get('label_summary', tkey)[:32]}...", fontsize=11, weight='bold')
        ax.set_xlabel("Dirichlet Term Weight (Beta)", fontsize=9)
        ax.grid(axis='x', linestyle='--', alpha=0.6)
        
        for p in ax.patches:
            w = p.get_width()
            ax.annotate(f"{w:.3f}", (w + 0.001, p.get_y() + p.get_height()/2),
                        ha='left', va='center', fontsize=8, color='#333333')

    # Hide unused subplots
    for j in range(idx + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.suptitle("PySpark Latent Dirichlet Allocation (LDA) - Top Keywords per Topic", fontsize=14, weight='bold', y=0.99)
    plt.tight_layout()
    
    save_path = os.path.join(output_dir, "lda_topic_top_terms.png")
    plt.savefig(save_path)
    plt.close()
    print(f"[+] Saved: {save_path}")

def plot_publisher_heatmap(summary_data, output_dir="visualizations"):
    """Plot 3: Publisher vs Topic coverage heatmap."""
    pub_dist = pd.DataFrame(summary_data.get("publisher_distribution", []))
    if pub_dist.empty:
        return

    pivot_table = pub_dist.pivot(index="publication", columns="topic_label", values="count").fillna(0)
    
    # Calculate percentage share per publisher (row-normalized)
    pivot_pct = pivot_table.div(pivot_table.sum(axis=1), axis=0) * 100

    plt.figure(figsize=(12, 6.5))
    sns.heatmap(
        pivot_pct,
        annot=True,
        fmt=".1f",
        cmap="YlGnBu",
        cbar_kws={'label': 'Publisher Editorial Share (%)'},
        linewidths=0.5,
        linecolor='white'
    )
    plt.title("Publisher Editorial Focus: Topic Share Cross-Tabulation Matrix (%)", fontsize=13, weight='bold', pad=15)
    plt.xlabel("Extracted Latent Topic", fontsize=11, labelpad=10)
    plt.ylabel("News Publication Agency", fontsize=11)
    plt.xticks(rotation=25, ha='right', fontsize=9)
    plt.yticks(rotation=0, fontsize=10)
    plt.tight_layout()

    save_path = os.path.join(output_dir, "publisher_topic_heatmap.png")
    plt.savefig(save_path)
    plt.close()
    print(f"[+] Saved: {save_path}")

def plot_temporal_trends(summary_data, output_dir="visualizations"):
    """Plot 4: Monthly topic trajectory over time."""
    trends = pd.DataFrame(summary_data.get("temporal_trends", []))
    if trends.empty:
        return

    # Create composite year-month period string
    trends["period"] = trends.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)
    
    # Aggregate monthly volume by topic
    pivot_trends = trends.pivot_table(index="period", columns="topic_label", values="article_count", aggfunc='sum').fillna(0)
    
    # Sort chronological periods
    pivot_trends = pivot_trends.sort_index()

    plt.figure(figsize=(13, 6))
    for col_name in pivot_trends.columns:
        plt.plot(pivot_trends.index, pivot_trends[col_name], marker='o', markersize=4, linewidth=2, label=col_name)

    plt.title("Temporal Trajectory: Monthly Article Volume by Topic (2024 - 2026)", fontsize=13, weight='bold', pad=15)
    plt.xlabel("Time Horizon (Year-Month)", fontsize=11, labelpad=10)
    plt.ylabel("Aggregated Monthly Article Count", fontsize=11)
    plt.xticks(rotation=45, ha='right', fontsize=9)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(title="Topic Domain", bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True)
    plt.tight_layout()

    save_path = os.path.join(output_dir, "temporal_topic_trends.png")
    plt.savefig(save_path)
    plt.close()
    print(f"[+] Saved: {save_path}")

def plot_sentiment_distribution(df_parquet, output_dir="visualizations"):
    """Plot 5: Sentiment Polarity Boxplot across Topics."""
    if df_parquet is None or "sentiment_polarity" not in df_parquet.columns:
        return

    plt.figure(figsize=(11, 5.5))
    palette = sns.color_palette("coolwarm", df_parquet["topic_label"].nunique())
    
    sns.boxplot(
        x="topic_label",
        y="sentiment_polarity",
        data=df_parquet,
        palette=palette,
        hue="topic_label",
        legend=False,
        showmeans=True,
        meanprops={"marker":"o", "markerfacecolor":"white", "markeredgecolor":"black", "markersize":"6"}
    )
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.8)
    plt.title("Lexical Sentiment Polarity Distribution across Topics (VADER Compound)", fontsize=13, weight='bold', pad=15)
    plt.xlabel("Extracted Latent Topic", fontsize=11, labelpad=10)
    plt.ylabel("Sentiment Score (-1.0 Negative to +1.0 Positive)", fontsize=11)
    plt.xticks(rotation=25, ha='right', fontsize=9)
    plt.tight_layout()

    save_path = os.path.join(output_dir, "topic_sentiment_distribution.png")
    plt.savefig(save_path)
    plt.close()
    print(f"[+] Saved: {save_path}")

def plot_wordclouds_grid(summary_data, output_dir="visualizations"):
    """Plot 6: Multi-panel Word Cloud representing extracted topics."""
    topics = summary_data.get("topics", {})
    if not topics:
        return

    num_topics = len(topics)
    fig, axes = plt.subplots(2, 3, figsize=(15, 9))
    axes = axes.flatten()

    for idx, (tkey, tdata) in enumerate(topics.items()):
        if idx >= 6:
            break
        ax = axes[idx]
        
        # Build frequency dictionary from terms and weights
        freq_dict = {term: weight for term, weight in zip(tdata["terms"], tdata["weights"])}
        
        wc = WordCloud(
            width=600,
            height=400,
            background_color='white',
            colormap='viridis',
            max_words=15
        ).generate_from_frequencies(freq_dict)

        ax.imshow(wc, interpolation='bilinear')
        ax.set_title(f"Topic {tdata['topic_id']}: {tdata.get('label_summary', tkey)[:28]}", fontsize=11, weight='bold')
        ax.axis('off')

    plt.suptitle("Semantic Keyword Clouds per Extracted Latent Topic", fontsize=14, weight='bold', y=0.98)
    plt.tight_layout()

    save_path = os.path.join(output_dir, "topic_wordclouds_grid.png")
    plt.savefig(save_path)
    plt.close()
    print(f"[+] Saved: {save_path}")

def generate_all_visualizations(
    summary_json_path="data/pipeline_analytics_summary.json",
    parquet_dir="data/processed_parquet",
    output_dir="visualizations"
):
    """Orchestrates generation of all visual analytical assets."""
    print("\n[*] Initializing Big Data Visualization Suite...")
    os.makedirs(output_dir, exist_ok=True)
    summary_data, df_parquet = load_data(summary_json_path, parquet_dir)

    if not summary_data:
        print("[!] Visualizations aborted: missing pipeline output data.")
        return False

    plot_topic_volume(summary_data, output_dir)
    plot_lda_top_terms(summary_data, output_dir)
    plot_publisher_heatmap(summary_data, output_dir)
    plot_temporal_trends(summary_data, output_dir)
    plot_sentiment_distribution(df_parquet, output_dir)
    plot_wordclouds_grid(summary_data, output_dir)

    print(f"\n[+] All visual analytics successfully generated and saved to: {output_dir}/")
    return True

if __name__ == "__main__":
    generate_all_visualizations()
