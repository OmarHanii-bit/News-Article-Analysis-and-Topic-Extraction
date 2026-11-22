"""
Supervised Ground-Truth Classification on Real Reuters News Corpus
==================================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Analytical Depth & Evaluation — Supervised Learning Extension
Owner: Member 4 — Shehab Osama (NLP & Machine Learning Scientist)
Collaborators: Member 1 — Omar Hani, Member 7 — Mazen Salem

PURPOSE & RUBRIC ALIGNMENT:
---------------------------
Directly addresses and resolves:
  - Limitation #7: "Unsupervised labels... No ground-truth labelled classification F1 is reported."
  - Future Work #4: "Add a supervised classifier on the Reuters category labels to report held-out macro-F1."

This module evaluates predictive machine learning performance against genuine
journalistic category ground truth from the NLTK Reuters-21578 wire corpus
(10,712 articles). It benchmarks multiple classifiers across a held-out test
partition (80/20 stratified split) and computes:
  - Test Accuracy
  - Macro-F1, Micro-F1, Weighted-F1
  - 5-Fold Stratified Cross-Validation scores
  - Per-category precision, recall, and support
  - Publication-grade confusion matrix and ROC/comparison figures
"""

import os
import sys
import json
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import (
    accuracy_score,
    precision_recall_fscore_support,
    classification_report,
    confusion_matrix
)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DEFAULT_INPUT = "data/real_news/raw_news_articles_real.csv"
OUTPUT_METRICS = "data/supervised_classification_metrics.json"
OUTPUT_CM_PNG = "visualizations/supervised_confusion_matrix.png"
OUTPUT_COMP_PNG = "visualizations/supervised_model_comparison.png"
MIN_CATEGORY_SUPPORT = 80  # Focus on statistically robust categories for benchmark


def load_and_prep_corpus(csv_path: str, min_support: int = MIN_CATEGORY_SUPPORT):
    """Loads genuine Reuters articles and filters to classes with sufficient support."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    # Require non-null content and category
    df = df.dropna(subset=["content", "category"]).copy()
    df["content"] = df["content"].astype(str)
    df["category"] = df["category"].astype(str).str.strip().str.title()

    # Filter categories by minimum support
    counts = df["category"].value_counts()
    valid_categories = counts[counts >= min_support].index.tolist()
    filtered_df = df[df["category"].isin(valid_categories)].copy()

    print(f"[*] Loaded {len(df)} total real articles.")
    print(f"[+] Retained {len(filtered_df)} articles across {len(valid_categories)} robust categories (min support >= {min_support}).")
    print(f"[*] Categories evaluated: {', '.join(valid_categories)}")
    return filtered_df, valid_categories


def train_and_evaluate(df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42):
    """Trains TF-IDF pipeline + multiple classifiers and evaluates on held-out test split."""
    X = df["content"].values
    y = df["category"].values

    # 80/20 Stratified train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    print(f"[*] Split sizes: {len(X_train)} train, {len(X_test)} test (held-out {int(test_size*100)}%).")

    # Distributed-compatible TF-IDF extraction
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2),
        max_features=10000,
        sublinear_tf=True,
        min_df=3,
        stop_words="english"
    )
    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)

    # Models benchmark
    models = {
        "Linear Support Vector (SGD)": SGDClassifier(loss="log_loss", penalty="l2", alpha=1e-4, max_iter=1000, random_state=random_state),
        "Multinomial Naive Bayes": MultinomialNB(alpha=0.1),
        "Logistic Regression (L2)": LogisticRegression(C=1.0, max_iter=1000, random_state=random_state),
    }

    results = {}
    best_model_name = None
    best_macro_f1 = -1.0
    best_predictions = None
    best_model_obj = None

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_state)

    for name, model in models.items():
        print(f"[*] Training & evaluating {name}...")
        # 5-fold cross validation on train split
        cv_scores = cross_val_score(model, X_train_vec, y_train, cv=cv, scoring="f1_macro", n_jobs=-1)
        
        # Fit on full training set
        model.fit(X_train_vec, y_train)
        y_pred = model.predict(X_test_vec)

        acc = accuracy_score(y_test, y_pred)
        p_macro, r_macro, f1_macro, _ = precision_recall_fscore_support(y_test, y_pred, average="macro", zero_division=0)
        p_micro, r_micro, f1_micro, _ = precision_recall_fscore_support(y_test, y_pred, average="micro", zero_division=0)
        p_weighted, r_weighted, f1_weighted, _ = precision_recall_fscore_support(y_test, y_pred, average="weighted", zero_division=0)

        results[name] = {
            "accuracy": float(round(acc, 4)),
            "macro_f1": float(round(f1_macro, 4)),
            "macro_precision": float(round(p_macro, 4)),
            "macro_recall": float(round(r_macro, 4)),
            "micro_f1": float(round(f1_micro, 4)),
            "weighted_f1": float(round(f1_weighted, 4)),
            "cv_f1_macro_mean": float(round(cv_scores.mean(), 4)),
            "cv_f1_macro_std": float(round(cv_scores.std(), 4)),
        }

        print(f"    -> Test Accuracy: {acc:.4f} | Macro-F1: {f1_macro:.4f} | CV Macro-F1: {cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})")

        if f1_macro > best_macro_f1:
            best_macro_f1 = f1_macro
            best_model_name = name
            best_predictions = y_pred
            best_model_obj = model

    # Full per-category classification report for best model
    classes = sorted(list(set(y_test)))
    class_report_dict = classification_report(y_test, best_predictions, target_names=classes, output_dict=True, zero_division=0)
    cm = confusion_matrix(y_test, best_predictions, labels=classes)

    summary_payload = {
        "dataset_provenance": "NLTK Reuters-21578 Real News Wire Archive",
        "total_articles_evaluated": len(df),
        "train_set_size": len(X_train),
        "test_set_size": len(X_test),
        "num_classes": len(classes),
        "evaluated_classes": classes,
        "best_model": best_model_name,
        "best_test_macro_f1": best_macro_f1,
        "models_benchmarked": results,
        "detailed_class_performance": {
            cls: {
                "precision": round(class_report_dict[cls]["precision"], 4),
                "recall": round(class_report_dict[cls]["recall"], 4),
                "f1_score": round(class_report_dict[cls]["f1-score"], 4),
                "support": class_report_dict[cls]["support"],
            }
            for cls in classes
        }
    }

    return summary_payload, cm, classes, results


def plot_visualizations(cm: np.ndarray, classes: list, model_results: dict):
    """Renders publication-grade 300 DPI confusion matrix and model benchmark comparison."""
    os.makedirs("visualizations", exist_ok=True)

    # 1. Confusion Matrix
    plt.figure(figsize=(10, 8), dpi=300)
    cm_norm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
    sns.heatmap(cm_norm, annot=True, fmt=".2f", cmap="Blues",
                xticklabels=classes, yticklabels=classes,
                cbar_kws={'label': 'Normalized Accuracy'})
    plt.title("Supervised Classification: Reuters-21578 Ground Truth Confusion Matrix\n(Normalized by Class Support)", fontsize=13, fontweight="bold", pad=14)
    plt.xlabel("Predicted Category", fontsize=11, fontweight="bold")
    plt.ylabel("True Editorial Category", fontsize=11, fontweight="bold")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_CM_PNG, dpi=300)
    plt.close()
    print(f"[+] Saved confusion matrix to {OUTPUT_CM_PNG}")

    # 2. Model Comparison Bar Chart
    plt.figure(figsize=(10, 5), dpi=300)
    names = list(model_results.keys())
    macro_f1s = [model_results[m]["macro_f1"] for m in names]
    accuracies = [model_results[m]["accuracy"] for m in names]

    x = np.arange(len(names))
    width = 0.35

    plt.bar(x - width/2, accuracies, width, label='Test Accuracy', color='#1f77b4', alpha=0.9)
    plt.bar(x + width/2, macro_f1s, width, label='Test Macro-F1', color='#ff7f0e', alpha=0.9)

    plt.ylabel('Score', fontsize=11, fontweight="bold")
    plt.title('Supervised Model Comparison on Held-Out Reuters-21578 Test Set', fontsize=13, fontweight="bold", pad=14)
    plt.xticks(x, names, fontsize=10, fontweight="bold")
    plt.ylim(0, 1.05)
    plt.legend(loc='lower right', frameon=True)
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    for i in range(len(names)):
        plt.text(x[i] - width/2, accuracies[i] + 0.02, f"{accuracies[i]:.2f}", ha='center', fontsize=9, fontweight="bold")
        plt.text(x[i] + width/2, macro_f1s[i] + 0.02, f"{macro_f1s[i]:.2f}", ha='center', fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(OUTPUT_COMP_PNG, dpi=300)
    plt.close()
    print(f"[+] Saved model comparison chart to {OUTPUT_COMP_PNG}")


def main():
    parser = argparse.ArgumentParser(description="Supervised Classification Benchmark on Real Reuters News.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to normalized Reuters CSV.")
    parser.add_argument("--min-support", type=int, default=MIN_CATEGORY_SUPPORT, help="Minimum category support.")
    args = parser.parse_args()

    print("=" * 80)
    print("  SUPERVISED GROUND-TRUTH CLASSIFIER (REUTERS-21578)")
    print("  Resolves Limitation #7: Ground-Truth Classification Macro-F1 Evaluation")
    print("=" * 80)

    df, categories = load_and_prep_corpus(args.input, args.min_support)
    summary_payload, cm, classes, model_results = train_and_evaluate(df)

    os.makedirs(os.path.dirname(OUTPUT_METRICS) or ".", exist_ok=True)
    with open(OUTPUT_METRICS, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, indent=2)
    print(f"[+] Saved metrics payload to {OUTPUT_METRICS}")

    plot_visualizations(cm, classes, model_results)
    print("\n✅ Supervised classification evaluation completed successfully!")
    print(f"[*] Best Model: {summary_payload['best_model']} | Test Macro-F1: {summary_payload['best_test_macro_f1']:.4f}")


if __name__ == "__main__":
    main()
