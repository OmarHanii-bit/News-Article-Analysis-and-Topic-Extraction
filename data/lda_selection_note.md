# LDA topic-count reconciliation (k sweep vs. production k=6)

**Purpose.** One-page reconciliation of the reproducible model-selection sweep with the fixed `k=6` used in the production PySpark pipeline. It deliberately does **not** alter `data/lda_k_sweep.json`; the artifact is the source of truth.

## What was actually run

`src/lda_topic_selection.py` re-ran the synthetic-benchmark NLP stages (RegexTokenizer → StopWordsRemover → CountVectorizer) on `data/raw_news_articles.csv` (6,000 documents) and swept **k in [3, 12]** on a **438-term** feature space, with 20 Online Variational Bayes iterations and seed 42. For each k it recorded:

- PySpark MLlib `LDAModel.logPerplexity` (predictive fit, lower is better),
- an aggregated **UMass-style** document co-occurrence coherence over each topic's top 10 terms (higher, i.e. closer to 0, is better), and
- mean pairwise Jaccard distinctness of top-term sets (lower is less redundant).

No alternative coherence variant was computed anywhere in this sweep.

## What the artifact says

| Observation | Value in `data/lda_k_sweep.json` |
| :--- | :--- |
| Best mean UMass coherence | **k=5, −0.3270** (next best k=10, −0.3588) |
| Coherence at k=6 | **−0.3668** (not the peak) |
| Coherence at k=3 | −0.3683 |
| Perplexity | 5.1792 (k=3) → 4.7635 (k=10) → 4.7753 (k=12); explicitly **non-monotonic** |
| Distinctness at k=3 | 0.0175 (low redundancy) |
| `perplexity_monotone` | `false` |
| `selected_k` | **3** |
| Selection rule | Coherent "knee" (marginal coherence gain < 5% of the coherence span), rejecting k values that increase topic redundancy |

The knee rule selects **k=3**, the smallest k on the interpretability plateau; it does not claim that k=3 maximises raw coherence. The sweep's own `selection_rule` text is used verbatim rather than overwritten.

## Why production still uses k=6

The production pipeline keeps `k=6` for **engineering and interpretability reasons**, not because it was proven statistically optimal:

1. The synthetic benchmark is explicitly organised into **six domains**, and the dashboard/heatmap labels and cross-corpus comparisons (synthetic 6,000 articles vs. Reuters 10,712 articles) require a **fixed, comparable k**.
2. At k=6 the dominant-topic rollup produces **five coherent clusters plus one unused component**: Clinical Medicine & Genomics (Healthcare), Grid Decarbonization & Clean Energy (Climate), Quantum Physics & Advanced Materials (Science), Markets, Politics & Regulation (Business + Politics merged), and AI Infrastructure & Compute (Technology). Labels are assigned post-hoc from each cluster's majority `category`; the clustering itself remains unsupervised. This structure is interpretable for stakeholders and auditable via `metadata.label_diagnostics`.
3. The dominant-topic confidence is high (>99% mean), but that indicates decisive assignments at the configured k, not optimality of k itself.

This is an acknowledged trade-off: the sweep on 438 terms favours fewer, broader topics by its knee rule, while the deployed model trades a small coherence difference for benchmark alignment and comparability.

## Honest wording for reports and defenses

> A sweep over k in [3,12] was run with UMass-style coherence, log perplexity and distinctness on a 438-term feature space. Perplexity was non-monotonic; the statistical knee rule selected k=3. Production retains k=6 because the six-domain synthetic benchmark and cross-corpus comparability require a fixed k, and topic labels are interpretable at k=6. At k=6 the rollup yields five dominant clusters plus one unused component; labels are assigned post-hoc from each cluster's majority category. This is an engineering/interpretability choice, not a proven optimum; a 5,000-term rerun is future work.

**Evidence:** `src/lda_topic_selection.py`, `data/lda_k_sweep.json`, `visualizations/lda_k_sweep.png`, `data/pipeline_analytics_summary.json`.
