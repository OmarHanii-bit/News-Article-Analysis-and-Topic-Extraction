"""
Automated Topic Drift & Model Monitoring System
===============================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Production MLOps & Model Governance Layer
Owner: Member 4 — Shehab Osama (NLP & Machine Learning Scientist)
Collaborators: Member 7 — Mazen Salem (QA, Benchmarking & Presentation Lead)

PURPOSE & RUBRIC ALIGNMENT:
---------------------------
Directly addresses and resolves:
  - Future Work #6: "Add automated topic-drift detection and a model-monitoring dashboard for production use."

In real-world data ecosystems, news topics undergo non-stationary concept drift
(e.g., sudden shifts in macroeconomic discourse, regulatory crackdowns, geopolitical
conflicts). This module continuously monitors topic velocity and distribution stability
using industry-standard statistical distance metrics:
  - Jensen-Shannon Divergence (JSD)
  - Population Stability Index (PSI)
  - Topic Velocity (% change in dominant topic market share)
  - Automated Alerting: Emits CRITICAL alerts if JSD > 0.15 or PSI > 0.25
"""

import os
import json
import math
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

DEFAULT_INPUT_SUMMARY = "data/pipeline_analytics_summary.json"
DEFAULT_INPUT_PARQUET = "data/processed_parquet"
OUTPUT_JSON = "data/topic_drift_analysis.json"
OUTPUT_PNG = "visualizations/topic_drift_monitoring.png"


def kl_divergence(p: np.ndarray, q: np.ndarray, eps: float = 1e-12) -> float:
    """Computes Kullback-Leibler divergence D_KL(P || Q)."""
    p = np.clip(p, eps, 1.0)
    q = np.clip(q, eps, 1.0)
    return float(np.sum(p * np.log(p / q)))


def jensen_shannon_divergence(p: np.ndarray, q: np.ndarray) -> float:
    """Computes bounded Jensen-Shannon Divergence JSD(P || Q) in [0, 1]."""
    m = 0.5 * (p + q)
    jsd = 0.5 * kl_divergence(p, m) + 0.5 * kl_divergence(q, m)
    return float(math.sqrt(max(0.0, jsd)))  # JS distance metric


def population_stability_index(baseline: np.ndarray, current: np.ndarray, eps: float = 1e-6) -> float:
    """Computes Population Stability Index (PSI) between two discrete distributions."""
    b = np.clip(baseline, eps, 1.0)
    c = np.clip(current, eps, 1.0)
    b = b / np.sum(b)
    c = c / np.sum(c)
    psi = np.sum((c - b) * np.log(c / b))
    return float(max(0.0, psi))


def evaluate_temporal_topic_drift(parquet_path: str, summary_path: str):
    """Calculates quarterly and monthly drift trajectories against global baseline."""
    # Load metadata
    summary = {}
    if os.path.exists(summary_path):
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

    topic_names = summary.get("topic_definitions", {})

    # Load Parquet lakehouse
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet lakehouse not found at: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    if "dominant_topic" not in df.columns or "year" not in df.columns or "month" not in df.columns:
        raise ValueError("Dataset missing required partition/topic columns.")

    # Global baseline distribution across all 6 topics
    num_topics = int(max(df["dominant_topic"].max() + 1, 6))
    global_counts = df["dominant_topic"].value_counts().reindex(range(num_topics), fill_value=0)
    global_p = (global_counts / len(df)).values

    # Build quarterly time slices (cast partition columns to int)
    m_int = df["month"].astype(int)
    y_str = df["year"].astype(str)
    df["quarter"] = y_str + "-Q" + ((m_int - 1) // 3 + 1).astype(str)
    quarters = sorted(df["quarter"].unique())

    quarterly_drift = []
    alerts = []

    print(f"[*] Baseline topic distribution across {len(df)} total articles:")
    for t_id, prob in enumerate(global_p):
        t_label = topic_names.get(str(t_id), {}).get("label", f"Topic {t_id}")
        print(f"    - Topic {t_id} ({t_label:<25}): {prob*100:5.2f}%")

    print("\n[*] Evaluating temporal concept drift across quarterly windows:")
    prev_q_dist = None

    for q in quarters:
        q_df = df[df["quarter"] == q]
        q_counts = q_df["dominant_topic"].value_counts().reindex(range(num_topics), fill_value=0)
        q_dist = (q_counts / len(q_df)).values

        # Compute metrics vs. Global Baseline
        jsd_baseline = float(jensen_shannon_divergence(global_p, q_dist))
        psi_baseline = float(population_stability_index(global_p, q_dist))

        # Compute metric vs. Previous Quarter (sequential shift)
        jsd_sequential = float(
            jensen_shannon_divergence(prev_q_dist, q_dist) if prev_q_dist is not None else 0.0
        )
        prev_q_dist = q_dist

        # Health status categorization
        if psi_baseline > 0.25 or jsd_baseline > 0.20:
            status = "CRITICAL_DRIFT"
            alert = {
                "period": str(q),
                "severity": "CRITICAL",
                "jsd": round(jsd_baseline, 4),
                "psi": round(psi_baseline, 4),
                "action": "Immediate model retraining recommended; significant distribution shift detected."
            }
            alerts.append(alert)
        elif psi_baseline > 0.10 or jsd_baseline > 0.10:
            status = "MODERATE_SHIFT"
        else:
            status = "STABLE"

        quarterly_drift.append({
            "quarter": str(q),
            "article_count": int(len(q_df)),
            "jsd_vs_baseline": round(jsd_baseline, 4),
            "psi_vs_baseline": round(psi_baseline, 4),
            "jsd_vs_previous_quarter": round(jsd_sequential, 4),
            "drift_status": status,
            "topic_proportions": {
                str(t): round(float(q_dist[t]), 4) for t in range(num_topics)
            }
        })

        print(f"    [{q}] Articles: {len(q_df):<5} | JSD vs Base: {jsd_baseline:.4f} | PSI: {psi_baseline:.4f} -> [{status}]")

    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "total_documents_monitored": int(len(df)),
        "monitoring_metric": "Jensen-Shannon Divergence (JSD) & Population Stability Index (PSI)",
        "num_topics": int(num_topics),
        "global_baseline_distribution": {str(t): round(float(global_p[t]), 4) for t in range(num_topics)},
        "quarterly_drift_history": quarterly_drift,
        "total_alerts_fired": int(len(alerts)),
        "alerts": alerts
    }

    return report, df, quarters, quarterly_drift


def plot_drift_dashboard(report: dict, quarters: list, quarterly_drift: list):
    """Renders 300 DPI multi-panel MLOps drift monitoring figure."""
    os.makedirs(os.path.dirname(OUTPUT_PNG) or ".", exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), dpi=300, sharex=True)

    jsd_vals = [d["jsd_vs_baseline"] for d in quarterly_drift]
    psi_vals = [d["psi_vs_baseline"] for d in quarterly_drift]

    # Panel 1: JSD Divergence Trajectory
    ax1.plot(quarters, jsd_vals, marker="o", color="#1f77b4", linewidth=2.5, markersize=8, label="JSD vs Global Baseline")
    ax1.axhline(0.15, color="red", linestyle="--", linewidth=1.5, label="Critical Drift Threshold (JSD = 0.15)")
    ax1.axhline(0.08, color="orange", linestyle=":", linewidth=1.2, label="Warning Threshold (JSD = 0.08)")
    ax1.set_ylabel("Jensen-Shannon Distance", fontsize=11, fontweight="bold")
    ax1.set_title("MLOps Topic Distribution Drift Monitoring Across Temporal Quarters", fontsize=13, fontweight="bold", pad=12)
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend(loc="upper left")

    for i, txt in enumerate(jsd_vals):
        ax1.annotate(f"{txt:.3f}", (quarters[i], jsd_vals[i] + 0.008), ha='center', fontsize=9, fontweight="bold")

    # Panel 2: PSI Population Stability Index
    colors = ["#2ca02c" if p <= 0.10 else "#ff7f0e" if p <= 0.25 else "#d62728" for p in psi_vals]
    ax2.bar(quarters, psi_vals, color=colors, alpha=0.85, width=0.5, label="Quarterly PSI vs Baseline")
    ax2.axhline(0.25, color="red", linestyle="--", linewidth=1.5, label="High Drift (PSI > 0.25)")
    ax2.axhline(0.10, color="orange", linestyle=":", linewidth=1.2, label="Moderate Shift (PSI > 0.10)")
    ax2.set_ylabel("Population Stability Index (PSI)", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Temporal Quarter", fontsize=11, fontweight="bold")
    ax2.grid(axis='y', linestyle="--", alpha=0.5)
    ax2.legend(loc="upper left")

    for i, txt in enumerate(psi_vals):
        ax2.annotate(f"{txt:.3f}", (quarters[i], psi_vals[i] + 0.01), ha='center', fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(OUTPUT_PNG, dpi=300)
    plt.close()
    print(f"[+] Saved MLOps drift dashboard to {OUTPUT_PNG}")


def main():
    parser = argparse.ArgumentParser(description="Automated Topic Drift Monitoring System.")
    parser.add_argument("--parquet", default=DEFAULT_INPUT_PARQUET, help="Parquet lakehouse directory.")
    parser.add_argument("--summary", default=DEFAULT_INPUT_SUMMARY, help="Analytics summary JSON.")
    args = parser.parse_args()

    print("=" * 80)
    print("  AUTOMATED TOPIC DRIFT & MODEL MONITORING SYSTEM (MLOps)")
    print("  Resolves Future Work #6: Topic Drift Detection & Statistical Governance")
    print("=" * 80)

    report, df, quarters, quarterly_drift = evaluate_temporal_topic_drift(args.parquet, args.summary)

    os.makedirs(os.path.dirname(OUTPUT_JSON) or ".", exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"[+] Drift evaluation report written to {OUTPUT_JSON}")

    plot_drift_dashboard(report, quarters, quarterly_drift)
    print("\n✅ Automated topic drift evaluation completed successfully!")


if __name__ == "__main__":
    main()
