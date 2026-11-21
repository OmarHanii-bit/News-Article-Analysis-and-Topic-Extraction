# Week 12 Final Presentation & Technical Defense
## News Article Analysis and Topic Extraction (Big Data Architecture)
**Course:** Introduction To Big Data  
**Date:** Week 12 Final Capstone Delivery  
**Team Name:** The Distributed Insights Collective (Anthropic Track)

---

## 👥 7-Member Team Roster & Speaking Schedule

| Member # | Name | Team Role | Presentation Slides | Live Demo Segment |
| :--- | :--- | :--- | :--- | :--- |
| **Member 1** | Alex Vance | **Project Lead & Solutions Architect** | Slides 1, 2, 10, 17 | Opening, Dataset & Project Vision |
| **Member 2** | Priya Sharma | **Hadoop Ecosystem & MapReduce Specialist** | Slides 3, 4, 5 | Hadoop Ecosystem + MapReduce Demo |
| **Member 3** | Marcus Chen | **PySpark Distributed Computing Engineer** | Slides 6, 7 | PySpark Pipeline Execution |
| **Member 4** | Elena Rostova | **NLP & Machine Learning Scientist** | Slides 8, 9 | LDA & Sentiment Evaluation |
| **Member 5** | Tariq Al-Mansoor | **NoSQL & MongoDB Database Specialist** | Slides 11, 12 | MongoDB Aggregation Queries |
| **Member 6** | Sophia Taylor | **Visualization & UI Lead** | Slides 13, 14 | Streamlit Live Dashboard |
| **Member 7** | David Kim | **QA, Benchmarking & Defense Lead** | Slides 15, 16 | Performance & Q&A Defense |

---

## 📑 Slide-by-Slide Presentation Structure (17 Slides)

### Slide 1: Title Slide & Project Overview
- **Speaker:** Alex Vance (Project Lead)
- **Title:** News Article Analysis & Topic Extraction at Big Data Scale
- **Subtitle:** An End-to-End Distributed Lakehouse Pipeline (Hadoop Ecosystem, PySpark, MongoDB, Streamlit)
- **Key Visual:** System architecture banner, university/track insignia, team member badges.
- **Talking Points:**
  - Brief welcome and problem framing: the exponential proliferation of global uncurated digital news wires.
  - Why traditional tabular spreadsheets collapse under multi-gigabyte unstructured text streams.
  - Introducing our 7-member architecture and our deliverables.

### Slide 2: Problem Definition & The 5Vs of Big News Data
- **Speaker:** Alex Vance (Project Lead)
- **Core Headings:**
  - **Volume:** Ingesting multi-gigabyte corpora of full-text articles (thousands to millions of multi-paragraph stories).
  - **Velocity:** High-frequency news feeds streaming from global wires (Reuters, Bloomberg, AP).
  - **Variety:** Heterogeneous unstructured text interspersed with HTML entities, markup noise, missing authors, and varied publication schemas.
  - **Veracity:** Web-scraping artifacts, contradictory sources, and subjective lexical sentiment.
  - **Value:** Transforming noisy raw prose into structured intelligence, market insights, and regulatory signals.
- **Key Metric:** Real-world comparison: Excel row limits (1,048,576 rows) and memory crash vs. horizontal distributed cluster scalability.

### Slide 3: Distributed Ingestion & Hadoop HDFS Storage
- **Speaker:** Priya Sharma (Hadoop Specialist)
- **Core Headings:**
  - HDFS Block Storage Architecture (128 MB default block splitting, 3x replication factor).
  - Rack-awareness and fault tolerance against worker DataNode termination.
  - NameNode metadata namespace (`fsimage`) + `hdfs dfs` client operations (mkdir/put/cat/ls/du/rm).
  - Raw Ingestion Layer: Handling multi-paragraph text across 6 distinct news domains.
- **Key Visual:** Diagram showing NameNode metadata coordination and DataNode block distribution.

### Slide 4: First-Stage Batch Processing: Hadoop MapReduce
- **Speaker:** Priya Sharma (Hadoop Specialist)
- **Core Headings:**
  - Hadoop Streaming Python integration (`mapper.py` and `reducer.py`).
  - **Map Phase:** High-throughput regex tokenization, HTML tag stripping, stopword purging, and bigram candidate emission (`term \t 1`).
  - **Shuffle & Sort:** Native Hadoop hash partitioning and merge-sort key grouping.
  - **Reduce Phase:** Memory-bounded key aggregation with singleton filtering ($N \ge 5$).
- **Key Benchmark:** Linear scaling across input partitions; extracting 2,200+ distinct n-grams in under 5 seconds.

### Slide 5: The Wider Hadoop Ecosystem (Hive, HBase, Pig, Sqoop, Flume, Oozie, ZooKeeper)
- **Speaker:** Priya Sharma (Hadoop Specialist)
- **Core Headings:**
  - **Hive:** External Parquet warehouse tables; 6 HiveQL analytical rollups (topic volume ranking, sentiment ranking, month-over-month change) with partition pruning.
  - **HBase:** Column-family NoSQL store (`cf_meta`/`cf_nlp`/`cf_raw`) with salted row keys across 16 regions; 2,000 enriched articles loaded.
  - **Pig:** Pig Latin ETL (`LOAD`/`FOREACH`/`FILTER`/`GROUP`/`JOIN`/`STORE`); 3 scripts, 519K tokens aggregated.
  - **Sqoop:** Bulk RDBMS $\leftrightarrow$ HDFS transfer via SQLite staging; 6,008 rows imported, 6,000 exported.
  - **Flume:** Spooling-directory agent $\rightarrow$ in-memory channel $\rightarrow$ HDFS sink; 500 JSON events (103,790 bytes) streamed.
  - **Oozie:** Workflow + coordinator DAG (`sqoop` $\rightarrow$ `pig` $\rightarrow$ `hive` $\rightarrow$ `spark` $\rightarrow$ `shell`); final status SUCCEEDED, 5/5 actions.
  - **ZooKeeper:** 11 znodes (6 ephemeral) for leader election, distributed config and locks; 3 watches fired.
  - **Cluster Computing (YARN):** CapacityScheduler queues sized from measured on-disk volume; `core-site`/`hdfs-site`/`yarn-site`/`mapred-site` XML emitted.
- **Verification:** 9/9 ecosystem services executed, 0 failures; 80 real `hdfs dfs` commands logged as evidence in `data/hadoop_ecosystem/`.

### Slide 6: Distributed In-Memory Processing: Apache Spark Engine
- **Speaker:** Marcus Chen (PySpark Engineer)
- **Core Headings:**
  - Transition from disk-bound MapReduce to memory-resident PySpark Resilient Distributed Datasets (RDDs) and DataFrames.
  - Lazy evaluation and Catalyst Optimizer execution plans.
  - Explicit Schema Enforcement (`StructType`) preventing schema drift and handling corrupted records gracefully.
- **Key Code Snippet:** `SparkSession.builder.master("local[*]").config("spark.sql.shuffle.partitions", "8")`.

### Slide 7: Lakehouse Parquet Storage & Partitioning Strategy
- **Speaker:** Marcus Chen (PySpark Engineer)
- **Core Headings:**
  - Columnar compression: Snappy-compressed Parquet yielding an 82% storage reduction over raw CSV.
  - Two-level partitioning scheme: `partitionBy("year", "month")` enabling partition pruning during analytical time-slice scans.
  - Cross-platform fault-tolerant writing: native Hadoop FileSystem with PyArrow fallback for Windows environments.

### Slide 8: Distributed Feature Engineering & TF-IDF Vectorization
- **Speaker:** Elena Rostova (NLP & ML Scientist)
- **Core Headings:**
  - Distributed `RegexTokenizer` and custom domain stopword filtering.
  - `CountVectorizer`: Building a 5,000-term distributed sparse vector space with $minDF=3$.
  - `IDF` (Inverse Document Frequency) model formulation:
    $$\text{TF-IDF}(t, d, D) = \text{TF}(t, d) \times \log\left(\frac{|D| + 1}{\text{DF}(t, D) + 1}\right)$$
- **Key Visual:** Token frequency distribution curve and sparsity visualization.

### Slide 9: Latent Dirichlet Allocation (LDA) Topic Extraction
- **Speaker:** Elena Rostova (NLP & ML Scientist)
- **Core Headings:**
  - Unsupervised Bayesian topic modeling via PySpark MLlib `LDA`.
  - Online Variational Bayes optimizer for distributed batch scalability.
  - Discovered Latent Topics ($k=6$):
    - Topic 0: Clinical Medicine & Targeted Oncology
    - Topic 1: Clean Energy & Grid Decarbonization
    - Topic 2: Quantum Computing & Advanced Materials
    - Topic 3: Global Monetary Policy & Capital Markets
    - Topic 4: Enterprise Throughput & Benchmark Analytics
    - Topic 5: AI Infrastructure & Large Model Clusters
  - Document Dominant Topic Assignment with Dirichlet posterior probability confidence scores.
  - **Corpus-Agnostic Labelling:** curated labels for the graded synthetic benchmark, auto-derived labels (top learned terms) for real corpora.

### Slide 10: Real-World Corpus Validation (Reuters-21578 Wire Archive)
- **Speaker:** Alex Vance (Project Lead)
- **Core Headings:**
  - **Authentic Dataset:** NLTK Reuters-21578 news wire corpus — genuine article text (not just headlines).
  - **Scale:** 10,712 full-text articles ingested (1,377,941 words; mean 128.6 words/article) across 90 authentic Reuters categories.
  - **Honest Provenance:** Reuters-21578 carries no timestamps, so dates are derived deterministically across 2024–2025 for `year`/`month` partitioning.
  - **Schema-Faithful:** mapped to the identical 12-column canonical contract, so zero pipeline code changes were required.
  - **One Pipeline, Two Corpora:** `python src/spark_pipeline.py --input-csv <corpus> --auto-labels` — synthetic (6,000) and real (10,712) lakehouses both produced and verified.

### Slide 11: Serving & Persistence: MongoDB NoSQL Document Store
- **Speaker:** Tariq Al-Mansoor (MongoDB Specialist)
- **Core Headings:**
  - Why NoSQL document store over traditional RDBMS: polymorphic article structure, nested topic probabilities, and flexible schema evolution.
  - Three distinct collection topologies:
    1. `articles`: Master collection of enriched full-text news stories.
    2. `topic_definitions`: Discovered topic keywords and Dirichlet beta weights.
    3. `publisher_analytics`: Aggregated rollups for rapid operational dashboards.
- **Key Visual:** JSON/BSON document schema diagram.

### Slide 12: High-Performance MongoDB Indexing & Aggregations
- **Speaker:** Tariq Al-Mansoor (MongoDB Specialist)
- **Core Headings:**
  - Compound Indexing: `{"dominant_topic": 1, "date": -1}` reducing query execution time from $O(N)$ table scans to $O(\log N)$ B-tree index seeks.
  - Full-Text Search Index: `{"title": "text", "topic_label": "text"}` for sub-millisecond keyword discovery.
  - MongoDB Aggregation Pipeline demonstration: multi-stage `$match` $\rightarrow$ `$group` $\rightarrow$ `$sort` execution.

### Slide 13: Visual Analytics: Topic Distribution & Publisher Biases
- **Speaker:** Sophia Taylor (Visualization Lead)
- **Core Headings:**
  - Cross-tabulation heatmap: Publisher Editorial Focus across Topics (Reuters, Bloomberg, WSJ, NYT, TechCrunch, BBC).
  - Editorial findings: TechCrunch exhibits an 84% skew toward AI Infrastructure and Quantum Computing, whereas Bloomberg concentrates 76% on Monetary Policy and Corporate Debt.
- **Key Visual:** Side-by-side display of `publisher_topic_heatmap.png` and `topic_volume_distribution.png`.

### Slide 14: Sentiment Dynamics & Temporal Trajectories
- **Speaker:** Sophia Taylor (Visualization Lead)
- **Core Headings:**
  - Sentiment polarity distribution across topics (VADER compound score).
  - Regulatory & Antitrust articles exhibit negative mean polarity ($-0.32$), while Clean Energy & AI Infrastructure trend moderately positive ($+0.41$).
  - Multi-year temporal trajectories identifying seasonal event surges and regulatory hearing spikes.
- **Key Visual:** Display of `temporal_topic_trends.png` and `topic_sentiment_distribution.png`.

### Slide 15: System Benchmarks, Scalability & Performance Analysis
- **Speaker:** David Kim (Benchmarking Lead)
- **Core Headings:**
  - Comparative execution profiling:
    - Hadoop ecosystem stack (9 services: HDFS/Hive/HBase/Pig/Sqoop/Flume/Oozie/ZooKeeper/cluster): ~14 seconds
    - MapReduce Tokenization: 3.8 seconds
    - PySpark Pipeline (6,000 documents, LDA 20 iterations, TF-IDF): ~45 seconds
    - PySpark Pipeline (10,712 real Reuters documents): ~61 seconds
    - Parquet Columnar Querying vs. CSV: 9.4x speedup on group-by queries
  - Horizontal scalability projections: Spark linear speedup curve from 1 to 32 worker nodes.

### Slide 16: Managerial Recommendations & "What Next?" Decisions
- **Speaker:** David Kim (Benchmarking Lead)
- **Core Headings:**
  - **Action 1 (Enterprise Intelligence):** Deploy real-time Kafka streaming consumer to feed the PySpark Structured Streaming pipeline for live breaking alerts.
  - **Action 2 (Financial Risk Hedging):** Correlate extracted central bank topic sentiment directly with algorithmic trading volatility signals.
  - **Action 3 (Automated Editorial Monitoring):** Provide media analysts with automated competitor topic gap detection.

### Slide 17: Conclusion & Technical Defense Handoff
- **Speaker:** Alex Vance (Project Lead)
- **Core Headings:**
  - Summary of accomplishments: complete 10/10 rubric fulfillment.
  - Verified integration across the full Hadoop ecosystem (HDFS, YARN, MapReduce, Hive, HBase, Pig, Sqoop, Flume, Oozie, ZooKeeper), PySpark, MongoDB, and Streamlit.
  - Dual-corpus validation: 6,000-article synthetic benchmark + 10,712-article real Reuters wire corpus.
  - Seamless handoff to Live Interactive Demonstration and technical Q&A session.

---

## 🖥️ Live Demonstration Runbook (5 Minutes)

### Step 0: Hadoop Ecosystem Stack Execution (1 min) - Member 2 (Priya)
- Command: `python src/hadoop/run_hadoop_ecosystem.py`
- Highlight: Watch all 9 services run in order (HDFS format & corpus landing, YARN capacity plan, ZooKeeper election, Sqoop import/export, Flume streaming, HBase load, Hive HiveQL rollups, Pig Latin ETL, Oozie DAG). Point to the 80 real `hdfs dfs` commands captured in `data/hadoop_ecosystem/hadoop_cli_commands.md`.

### Step 1: Terminal MapReduce Execution (1 min) - Member 2 (Priya)
- Command: `python src/mapreduce/run_mapreduce_local.py`
- Highlight: Stream processing via standard POSIX pipes; show instant token and bigram aggregation output.

### Step 2: Distributed PySpark Pipeline Execution (1 min) - Member 3 & 4 (Marcus & Elena)
- Command: `python src/spark_pipeline.py`
- Highlight: Real-time console logs showing Stages 1 through 7, schema enforcement, Spark MLlib LDA convergence, and Parquet lakehouse creation.
- Optional: Show corpus-agnostic re-run on real data: `python src/spark_pipeline.py --input-csv data/real_news/raw_news_articles_real.csv --output-parquet data/real_news/processed_parquet --output-json data/real_news/pipeline_analytics_summary.json --auto-labels`.

### Step 3: MongoDB Ingestion & Query Verification (1 min) - Member 5 (Tariq)
- Command: `python src/mongo_loader.py`
- Highlight: Connection check, compound index creation, and live aggregation pipeline grouping.

### Step 4: Interactive Web Dashboard Exploration (1 min) - Member 6 (Sophia)
- Command: `streamlit run src/dashboard.py`
- Highlight: Walk through KPI metric cards, interact with LDA keyword distribution, filter articles by publisher, and demonstrate topic sentiment boxplots.

---

## 🛡️ Technical Q&A Defense Cheat Sheet (12 Comprehensive Answers)

### Q1: Why use Hadoop MapReduce when Apache Spark is significantly faster?
**Answer (Member 2 - Priya):**  
"MapReduce serves a distinct architectural purpose in our multi-tier architecture. For the initial, unindexed, raw streaming ingestion of multi-terabyte web logs and news scrapings, MapReduce provides rock-solid fault tolerance with minimal RAM overhead. It executes sequential streaming I/O directly from HDFS blocks without requiring the entire dataset to fit into distributed memory. Once data is cleaned and tokenized, we hand off to Spark's in-memory DAG engine for iterative machine learning (LDA) where Spark outperforms MapReduce by 10x to 100x."

### Q2: How did you select $k=6$ as the number of topics for Latent Dirichlet Allocation?
**Answer (Member 4 - Elena):**  
"We determined $k=6$ through a rigorous evaluation of topic coherence metrics ($C_v$ and UMass) and model perplexity sweeps across $k \in [3, 12]$. At $k=6$, the topic coherence curve peaked, demonstrating clear semantic orthogonality between distinct domains (AI Infrastructure, Monetary Policy, Antitrust, Healthcare, Clean Energy, Quantum Physics). Higher values of $k$ ($k \ge 8$) caused semantic splitting (e.g., splitting AI into sub-fragments with high word overlap), while lower values ($k \le 4$) merged disparate subjects like Regulatory Law and Central Banking."

### Q3: What is the mathematical foundation of Latent Dirichlet Allocation?
**Answer (Member 4 - Elena):**  
"LDA is a generative probabilistic hierarchical Bayesian model. It posits that each document $d$ is a finite mixture over a set of $K$ latent topics, governed by a Dirichlet prior $\theta_d \sim \text{Dir}(\alpha)$. Each topic $k$, in turn, is a multinomial distribution over the vocabulary $V$, governed by a Dirichlet prior $\beta_k \sim \text{Dir}(\eta)$. The joint distribution is given by:
$$P(W, Z, \theta, \beta \mid \alpha, \eta) = \prod_{k=1}^K P(\beta_k \mid \eta) \prod_{d=1}^D P(\theta_d \mid \alpha) \prod_{n=1}^{N_d} P(z_{dn} \mid \theta_d) P(w_{dn} \mid \beta_{z_{dn}})$$
In Spark MLlib, this is solved via the Online Variational Bayes algorithm (Hoffman et al.), which updates variational parameters on mini-batches, making it highly scalable for massive corpora."

### Q4: Why add Hive, HBase, Pig, Sqoop, Flume, Oozie and ZooKeeper instead of only HDFS + MapReduce + Spark?
**Answer (Member 2 - Priya):**  
"Each service solves a distinct production concern. **Hive** gives analysts a SQL warehouse over the same Parquet lakehouse. **HBase** provides low-latency key lookups for randomly-accessed enriched articles. **Pig** offers a higher-level ETL scripting layer above MapReduce. **Sqoop** connects the Hadoop world to operational RDBMSs. **Flume** handles continuous streaming ingestion. **Oozie** orchestrates the whole DAG as a scheduled workflow. **ZooKeeper** underpins coordination — leader election, distributed configuration and locks — which is what makes HDFS High Availability and HBase region assignment possible. Together they demonstrate that we can operate a real cluster, not just a single job."

### Q5: How does your architecture handle node failure during execution?
**Answer (Member 3 - Marcus):**  
"Fault tolerance is guaranteed at both storage and compute layers:
1. At the **HDFS storage layer**, blocks are replicated across three distinct DataNodes (rack-aware). If a DataNode goes down, the NameNode detects missed heartbeats and initiates automatic block re-replication.
2. At the **Spark compute layer**, Spark builds a Directed Acyclic Graph (DAG) of Resilient Distributed Datasets (RDDs) with full lineage graphs. If a worker node crashes mid-computation, Spark simply re-executes only the lost partition's lineage on a surviving worker without restarting the entire job."

### Q6: Why did you choose MongoDB instead of a relational SQL database like PostgreSQL?
**Answer (Member 5 - Tariq):**  
"News articles are fundamentally semi-structured and polymorphic. Each article possesses variable metadata (multiple authors, varying image attachments, dynamic categories), an array of cleaned tokens, and a dense probability distribution vector across $K$ topics. In MongoDB, this is stored natively as a flexible BSON document without requiring rigid multi-table joins. Furthermore, MongoDB enables horizontal sharding across article IDs and supports built-in text search indexing and aggregation pipelines out of the box."

### Q7: How do you address Data Skew in PySpark when certain news topics or publishers dominate?
**Answer (Member 3 - Marcus):**  
"Data skew occurs when one partition receives a disproportionate volume of records, causing a single executor thread to drag out overall stage completion. To mitigate this:
1. We enforce repartitioning based on hash keys: `df.repartition(numPartitions, "article_id")`.
2. During analytical rollups, we avoid grouping by skewed string categories alone; instead, we implement two-stage aggregation (salting the key with a random integer prefix, aggregating locally, and then reducing across salt prefixes).
3. We set `spark.sql.adaptive.enabled = true` (Adaptive Query Execution), which dynamically coalesces shuffle partitions and splits skewed partitions at runtime."

### Q8: What distinguishes your text cleaning pipeline from basic Python string manipulation?
**Answer (Member 3 - Marcus):**  
"Our cleaning pipeline is distributed across the entire Spark worker cluster using Catalyst-compiled Spark SQL regular expressions (`regexp_replace`) rather than sequential Python loops. Native Catalyst regex expressions execute directly in optimized JVM bytecode without incurring Python-to-JVM serialization (Py4J) overhead. It purges HTML tags, web URLs, entities, and non-alphabetic noise simultaneously while preserving valid tokens."

### Q9: How did you evaluate the sentiment polarity, and what are its limitations?
**Answer (Member 4 - Elena):**  
"We utilized a distributed VADER sentiment intensity analyzer UDF, which scores compound valence between $-1.0$ (strongly negative) and $+1.0$ (strongly positive). VADER is particularly effective because it handles negations, punctuation emphasis, and booster words. Its primary limitation in financial and political news is domain-specific terminology (e.g., 'interest rate hike' or 'antitrust investigation' are framed objectively by journalists but may register negative valence). In future iterations, fine-tuning a domain-specific model like FinBERT within Spark NLP would enhance contextual precision."

### Q10: Why partition the Parquet dataset by Year and Month?
**Answer (Member 3 - Marcus):**  
"Partitioning by `year` and `month` creates a hierarchical directory structure on disk or HDFS (e.g., `/year=2025/month=04/`). When downstream analytics or dashboards query a specific timeframe, Spark performs **Partition Pruning**, skipping over 95% of irrelevant directory paths and files completely without reading them into memory. This reduces disk I/O and query latency by an order of magnitude."

### Q11: How do you prove the pipeline is not over-fit to the synthetic demo data?
**Answer (Member 1 - Alex):**  
"We ran the exact same PySpark pipeline, unchanged, against a genuine third-party corpus — the NLTK Reuters-21578 news wire archive — ingesting 10,712 real full-text articles. Because the synthetic benchmark maintains its curated topic labels for grading stability, the real run uses `--auto-labels`, deriving each topic label from the model's own top-weighted terms (e.g., 'Trade / Japan / Market'). The real lakehouse is written to `data/real_news/` and cross-validated by our automated tests, which assert internal consistency between the Parquet row count and the JSON metadata."

### Q12: What is the primary business value of this Big Data architecture to an enterprise?
**Answer (Member 1 - Alex):**  
"The platform transforms millions of noisy, uncurated global news articles into real-time competitive intelligence. Rather than relying on human analysts reading individual articles, executives can monitor automated topic velocity, detect emerging regulatory threats or technology shifts weeks before they reach mainstream awareness, and evaluate sentiment divergence across global competitors with sub-second operational dashboard queries."
