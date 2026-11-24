"""
Dataset Generator & Kaggle Ingestion Preprocessor
=================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Authors: 7-Member Big Data Engineering & Analytics Team

Purpose:
--------
Generates a realistic, large-scale, heterogeneous news articles corpus containing full
article text (not just headlines). In addition, provides utility routines to validate,
profile, and ingest Kaggle datasets (such as 'All the News' 143k/2.7M articles).

Each article contains rich, messy, multi-paragraph text across 6 core domains:
1. Artificial Intelligence & Cloud Computing (Technology)
2. Global Monetary Policy & Financial Markets (Business)
3. Regulatory Policy, Antitrust & Governance (Politics)
4. Biomedical Discoveries & Healthcare Innovations (Healthcare)
5. Renewable Energy & Climate Change (Environment)
6. Semiconductor Hardware & Quantum Computing (Science & Tech)
"""

import os
import sys
import random
import datetime
import pandas as pd
import numpy as np

def generate_realistic_news_dataset(
    output_csv_path: str = "data/raw_news_articles.csv",
    num_records: int = 6000,
    seed: int = 42
):
    """
    Synthesizes a realistic, high-volume news dataset with realistic text noise,
    simulating uncleaned scrapings from global news wires.
    
    Parameters:
    -----------
    output_csv_path : str
        Target destination path for the raw CSV dataset.
    num_records : int
        Number of full-text records to synthesize (default 6,000).
    seed : int
        Random state for reproducibility.
    """
    random.seed(seed)
    np.random.seed(seed)
    print(f"[*] Starting dataset generation: {num_records} records...")

    publishers = [
        "Reuters", "Associated Press", "Bloomberg", "The Wall Street Journal",
        "The New York Times", "TechCrunch", "Financial Times", "BBC News"
    ]
    
    authors = [
        "Sarah Jenkins", "Michael Chang", "Shehab Osama", "David Vance",
        "Amara Okafor", "Julian Sterling", "Hana Tanaka", "Carlos Mendez",
        "Rachel Green", "Liam O'Connor", "Mohammed Hossam", "Staff Correspondent"
    ]

    domains = {
        "Technology": {
            "topics": ["AI Infrastructure", "Generative Models", "Cloud Compute", "Autonomous Systems"],
            "keywords": ["neural networks", "transformer architecture", "GPU cluster", "inference latency", 
                         "Anthropic Claude", "parameter scaling", "synthetic data", "data pipeline", "open source"],
            "paragraphs": [
                "Enterprises are rapidly adopting foundation models to streamline high-throughput automated workloads. "
                "Engineering teams report significant improvements in developer productivity, yet inference latency "
                "and distributed cluster synchronization remain formidable operational bottlenecks.",
                "Cloud infrastructure providers are aggressively expanding specialized compute clusters equipped with modern "
                "tensor processing accelerators. Data engineers emphasize that robust ETL pipelines and vector indexing "
                "are mandatory before deploying autonomous agentic workflows into production environments.",
                "Regulatory bodies in the European Union and North America have published guidelines scrutinizing large-scale "
                "model provenance, algorithmic transparency, and data ingestion compliance."
            ]
        },
        "Business": {
            "topics": ["Central Bank Rates", "Corporate Earnings", "Sovereign Debt", "Supply Chain Capital"],
            "keywords": ["interest rate hike", "inflation index", "treasury yields", "capital expenditure", 
                         "balance sheet", "equity markets", "mergers and acquisitions", "liquidity ratio"],
            "paragraphs": [
                "Global financial markets exhibited heightened volatility following the latest monetary policy conference. "
                "Analysts scrutinize key inflation indicators and labor market resilience as central banks calibrate benchmark yields.",
                "Multinational corporations are restructuring balance sheets to mitigate escalating cost-of-capital pressures. "
                "Executive leadership highlighted prudent capital allocation strategies during quarterly earnings calls.",
                "Supply chain realignments across Pacific trade corridors continue to influence inventory margins and working capital."
            ]
        },
        "Politics": {
            "topics": ["Antitrust Litigation", "Legislative Reform", "Trade Agreements", "Election Dynamics"],
            "keywords": ["congressional hearing", "antitrust lawsuit", "regulatory compliance", "bilateral treaty", 
                         "foreign policy", "bipartisan coalition", "sanctions regime", "statutory authority"],
            "paragraphs": [
                "Lawmakers convened public hearings to interrogate platform consolidation and competitive market access. "
                "Witnesses presented detailed empirical analyses addressing consumer pricing and digital interoperability.",
                "Diplomatic delegations negotiated bilateral economic pacts aimed at standardizing cross-border intellectual property rules.",
                "Subcommittees advanced bipartisan legislation introducing strict audit requirements for automated commercial systems."
            ]
        },
        "Healthcare": {
            "topics": ["Clinical Drug Trials", "Genomic Medicine", "Epidemiological Surveillance", "Biotech Venture"],
            "keywords": ["phase III trial", "monoclonal antibodies", "FDA approval", "oncology biomarkers", 
                         "genomic sequencing", "clinical efficacy", "placebo cohort", "immunotherapy"],
            "paragraphs": [
                "Clinical researchers announced breakthrough phase III trial findings demonstrating statistically significant "
                "improvements in overall patient survival rates for novel targeted immunotherapy regimens.",
                "Genomic sequencing technologies continue to lower molecular profiling costs, enabling personalized diagnostic "
                "protocols across major hospital networks and oncology treatment centers.",
                "Healthcare regulators expedited review pathways for breakthrough therapies targeting rare metabolic disorders."
            ]
        },
        "Climate": {
            "topics": ["Grid Decarbonization", "Carbon Sequestration", "Offshore Wind", "Battery Storage"],
            "keywords": ["gigawatt capacity", "photovoltaic efficiency", "carbon capture", "lithium-ion battery", 
                         "smart grid resilience", "net-zero targets", "renewable subsidies", "emissions baseline"],
            "paragraphs": [
                "Energy grid operators integrated record volumes of renewable power capacity, supported by industrial-scale "
                "battery storage installations and modern transmission infrastructure upgrades.",
                "Climate scientists and engineering consortiums published field evaluation data on direct air capture systems, "
                "underscoring the thermodynamic challenges and energetic trade-offs inherent in scaling extraction.",
                "Municipal governments adopted revised building codes prioritizing distributed rooftop solar and electrified heating."
            ]
        },
        "Science": {
            "topics": ["Quantum Computing", "Deep Space Missions", "Superconductivity", "Materials Informatics"],
            "keywords": ["quantum coherence", "cryogenic qubit", "james webb telescope", "exoplanet atmosphere", 
                         "spectroscopic analysis", "high-temperature superconductor", "lattice structure"],
            "paragraphs": [
                "Physicists achieved record coherence times in multi-qubit superconducting architectures, representing "
                "a milestone toward fault-tolerant quantum error correction and topological circuit design.",
                "Deep space astronomical observatories captured high-resolution spectroscopic signatures revealing atmospheric "
                "constituents of distant exoplanetary candidates.",
                "Computational material researchers utilized machine learning models to predict stable crystal phases for next-generation thermal conductors."
            ]
        }
    }

    # Synthetic noise artifacts commonly found in real-world web-scraped data
    noise_artifacts = [
        "<p>Read the original report at news-wire.net &amp; associates.</p>",
        "<br/><br/>Click here for full terms &amp; conditions.",
        "Sign up for our morning briefing newsletter: news@domain.org.",
        "Follow us on Twitter: @NewsWireDirect for breaking updates.",
        "ADVERTISEMENT: Subscribe today to unlock premium analytics reports.",
        "Copyright &copy; 2024-2026. All rights reserved across all jurisdictions.",
        "--- Editor's Note: Corrections appended at 14:30 GMT. ---",
        "&quot;Special Report&quot; - Unverified initial wire draft.",
        ""
    ]

    records = []
    base_date = datetime.date(2024, 1, 1)

    categories = list(domains.keys())
    # Assign varying category distribution to simulate real skew
    cat_weights = [0.25, 0.22, 0.18, 0.15, 0.12, 0.08]

    for idx in range(num_records):
        article_id = f"ART_{idx+100000:06d}"
        category = random.choices(categories, weights=cat_weights, k=1)[0]
        cat_data = domains[category]
        sub_topic = random.choice(cat_data["topics"])
        publisher = random.choice(publishers)
        author = random.choice(authors)
        
        # Random date spread over 730 days
        date_offset = random.randint(0, 730)
        art_date = base_date + datetime.timedelta(days=date_offset)
        
        # Build Title
        kw1, kw2 = random.sample(cat_data["keywords"], 2)
        title_templates = [
            f"{publisher} Analysis: {sub_topic} and {kw1.title()} in Focus",
            f"Global Perspective: How {kw2.title()} is Transforming {category}",
            f"Special Report: Emerging Trends in {sub_topic} and Market Dynamics",
            f"Breakthrough Developments in {kw1.title()} Stir Industry Debate",
            f"Inside the Numbers: {sub_topic} Reaches New Milestone"
        ]
        title = random.choice(title_templates)

        # Build Multi-paragraph Article Content (Realistic 250 - 600 words)
        selected_paragraphs = random.sample(cat_data["paragraphs"], k=random.randint(2, 3))
        # Inject additional sentences using keywords
        extra_sentences = [
            f"Industry observers noted that {kw1} remains integral to long-term scalability across competing organizations.",
            f"Recent benchmark metrics indicate that adoption of {kw2} surged over the past fiscal quarters.",
            f"Further empirical investigations into {category.lower()} methodologies highlight ongoing trade-offs between precision and throughput.",
            f"Senior delegates discussed the strategic implications of {kw1} during plenary panels yesterday."
        ]
        random.shuffle(extra_sentences)
        
        body_parts = selected_paragraphs + extra_sentences[:2]
        
        # In 40% of cases, inject web scraping noise (HTML tags, entities, newsletter disclaimers)
        if random.random() < 0.40:
            body_parts.insert(random.randint(0, len(body_parts)), random.choice(noise_artifacts))
        
        full_content = " \n\n".join(body_parts)

        # Introduce occasional missing or dirty fields to test pipeline robustness
        # 1.5% empty authors, 1% missing section
        if random.random() < 0.015:
            author = None
        
        record = {
            "article_id": article_id,
            "title": title,
            "author": author,
            "date": art_date.strftime("%Y-%m-%d"),
            "year": art_date.year,
            "month": art_date.month,
            "publication": publisher,
            "category": category,
            "sub_topic": sub_topic,
            "content": full_content,
            "word_count": len(full_content.split()),
            "url": f"https://www.{publisher.lower().replace(' ', '')}.com/article/{article_id.lower()}"
        }
        records.append(record)

    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_csv_path) or ".", exist_ok=True)
    df.to_csv(output_csv_path, index=False, encoding="utf-8")
    
    print(f"[+] Dataset successfully created at: {output_csv_path}")
    print(f"[+] Total records: {len(df):,}")
    print(f"[+] Memory footprint: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
    print(f"[+] Average word count per article: {df['word_count'].mean():.1f} words")
    print(f"[+] Category distribution:\n{df['category'].value_counts(normalize=True)}")
    return output_csv_path

if __name__ == "__main__":
    count = 6000
    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            pass
    generate_realistic_news_dataset(num_records=count)
