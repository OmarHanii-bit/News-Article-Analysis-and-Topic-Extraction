"""
Kaggle 'All the News' Direct Ingestion & Schema Adaptation Pipeline
===================================================================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Author: 7-Member Big Data Engineering Team (Member 1: Solutions Architect)

Description:
------------
Provides high-throughput, chunked streaming ingestion for real-world Kaggle
datasets (e.g., 'All the News' 143k articles, 'All the News 2.0' 2.7M articles).
Normalizes diverse Kaggle schema variants into the strict 12-attribute contract
required by our PySpark, MapReduce, and MongoDB lakehouse pipeline.

Key Capabilities:
-----------------
1. Memory-bounded streaming via Pandas chunksize (processes 10GB+ files in chunks)
2. Polymorphic column mapping (maps 'headline' -> 'title', 'article' -> 'content', etc.)
3. Temporal decomposition (extracts 'year' and 'month' for Parquet partitioning)
4. Data hygiene (purges null contents, normalizes dates, imputes missing authors)
5. CLI arguments for sampling (--limit) or complete ingestion (--input /path/to/kaggle.csv)
"""

import os
import json
import argparse
import pandas as pd

# Canonical 12-attribute pipeline schema (intentionally unchanged: the date
# imputation report is a SIDECAR file, never an extra CSV column).
TARGET_COLUMNS = [
    "article_id", "title", "author", "date", "year", "month",
    "publication", "category", "sub_topic", "content", "word_count", "url"
]

# Sidecar evidence for rows whose timestamp could not be parsed. These rows
# must be excluded from any temporal-trend conclusion.
DATE_IMPUTATION_REPORT_PATH = "data/date_imputation_report.json"
DEFAULT_IMPUTED_DATE = "2024-06-15"
_DATE_IMPUTATION_STATS = {"imputed_rows": 0, "total_rows": 0}

def map_kaggle_row_chunk(chunk, chunk_idx, default_category="General"):
    """
    Transforms a heterogeneous Kaggle chunk into the canonical 12-attribute schema.
    """
    # pandas' chunked reader keeps a CONTINUOUS index across chunks (0-4, 5-9, ...).
    # Re-basing to 0..n-1 keeps every Series assignment below row-aligned; without
    # this, chunks after the first union their index and inject NaN rows.
    chunk = chunk.reset_index(drop=True)
    df_mapped = pd.DataFrame()
    n_rows = len(chunk)

    # 1. Map article_id
    id_candidates = ["article_id", "id", "Unnamed: 0", "index"]
    matched_id = next((c for c in id_candidates if c in chunk.columns), None)
    if matched_id:
        df_mapped["article_id"] = chunk[matched_id].astype(str).radd("KAG_")
    else:
        df_mapped["article_id"] = [f"KAG_{chunk_idx}_{i:06d}" for i in range(n_rows)]

    # 2. Map title
    title_candidates = ["title", "headline", "title_clean"]
    matched_title = next((c for c in title_candidates if c in chunk.columns), None)
    df_mapped["title"] = chunk[matched_title].fillna("Untitled Article").astype(str) if matched_title else "Untitled Article"

    # 3. Map author
    author_candidates = ["author", "byline", "creator", "writer"]
    matched_author = next((c for c in author_candidates if c in chunk.columns), None)
    df_mapped["author"] = chunk[matched_author].fillna("Staff Correspondent").astype(str) if matched_author else "Staff Correspondent"

    # 4. Map content (CRITICAL: full article text)
    content_candidates = ["content", "article", "text", "body", "article_text"]
    matched_content = next((c for c in content_candidates if c in chunk.columns), None)
    if not matched_content:
        raise ValueError(f"Kaggle file missing full-text article content column. Found: {list(chunk.columns)}")
    df_mapped["content"] = chunk[matched_content].fillna("").astype(str)

    # 5. Filter out empty or trivially short articles
    df_mapped["word_count"] = df_mapped["content"].apply(lambda t: len(t.split()))
    df_mapped = df_mapped[df_mapped["word_count"] >= 15].copy()
    if df_mapped.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    # 6. Map date, year, month
    date_candidates = ["date", "published_at", "publish_date", "timestamp"]
    matched_date = next((c for c in date_candidates if c in chunk.columns), None)
    
    dates_extracted = []
    years_extracted = []
    months_extracted = []
    imputed_in_chunk = 0

    for d_val in df_mapped.index:
        raw_d = chunk.loc[d_val, matched_date] if matched_date else None
        parsed_d = None
        if pd.notnull(raw_d):
            try:
                parsed_d = pd.to_datetime(raw_d)
            except Exception:
                pass
        
        if parsed_d is not None and not pd.isnull(parsed_d):
            dates_extracted.append(parsed_d.strftime("%Y-%m-%d"))
            years_extracted.append(int(parsed_d.year))
            months_extracted.append(int(parsed_d.month))
        else:
            # Keep the canonical 12-column schema: count + report the imputation
            # in a sidecar JSON instead of adding an extra CSV flag column.
            imputed_in_chunk += 1
            dates_extracted.append(DEFAULT_IMPUTED_DATE)
            years_extracted.append(2024)
            months_extracted.append(6)

    _DATE_IMPUTATION_STATS["imputed_rows"] += imputed_in_chunk
    _DATE_IMPUTATION_STATS["total_rows"] += len(df_mapped)
    if imputed_in_chunk:
        print(f"    [!] WARNING: {imputed_in_chunk:,}/{len(df_mapped):,} rows in chunk "
              f"{chunk_idx} had missing/unparseable dates and were imputed to "
              f"{DEFAULT_IMPUTED_DATE}. Exclude them from temporal conclusions "
              f"(see {DATE_IMPUTATION_REPORT_PATH}).")

    df_mapped["date"] = dates_extracted
    df_mapped["year"] = years_extracted
    df_mapped["month"] = months_extracted

    # 7. Map publication
    pub_candidates = ["publication", "publisher", "source", "site"]
    matched_pub = next((c for c in pub_candidates if c in chunk.columns), None)
    df_mapped["publication"] = chunk.loc[df_mapped.index, matched_pub].fillna("Global Wire").astype(str) if matched_pub else "Global Wire"

    # 8. Map category and sub_topic
    cat_candidates = ["category", "section", "topic", "tag"]
    matched_cat = next((c for c in cat_candidates if c in chunk.columns), None)
    df_mapped["category"] = chunk.loc[df_mapped.index, matched_cat].fillna(default_category).astype(str) if matched_cat else default_category
    df_mapped["sub_topic"] = df_mapped["category"].astype(str) + " - General Beat"

    # 9. Map url
    url_candidates = ["url", "link", "web_url", "article_url"]
    matched_url = next((c for c in url_candidates if c in chunk.columns), None)
    if matched_url:
        df_mapped["url"] = chunk.loc[df_mapped.index, matched_url].fillna("https://www.news-wire.com").astype(str)
    else:
        df_mapped["url"] = df_mapped["article_id"].apply(lambda aid: f"https://www.news-wire.com/article/{aid.lower()}")

    # Reorder columns to canonical schema
    return df_mapped[TARGET_COLUMNS]


def write_date_imputation_report(report_path: str = DATE_IMPUTATION_REPORT_PATH) -> dict:
    """Writes sidecar JSON describing how many rows received a fallback date."""
    report = {
        "imputed_rows": _DATE_IMPUTATION_STATS["imputed_rows"],
        "total_rows": _DATE_IMPUTATION_STATS["total_rows"],
        "note": ("Rows with missing/unparseable dates were imputed to "
                 "2024-06-15 solely to populate year/month partition keys. "
                 "These rows MUST be excluded from temporal conclusions; the "
                 "canonical 12-column CSV deliberately carries no imputation flag."),
    }
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"[+] Date imputation report -> {report_path} "
          f"({report['imputed_rows']:,}/{report['total_rows']:,} rows)")
    return report


def ingest_kaggle_file(
    input_file_path: str,
    output_csv_path: str = "data/raw_news_articles.csv",
    chunk_size: int = 10000,
    max_records: int = None,
    dedup: bool = False
):
    """
    Ingests and adapts a real-world Kaggle news CSV file into the pipeline format.

    ``dedup`` (CLI: --dedup, default off) drops repeated article_id/url values
    across chunks before writing so the canonical corpus stays unique.
    """
    print("=" * 80)
    print("  KAGGLE 'ALL THE NEWS' DATASET INGESTION & NORMALIZATION")
    print("=" * 80)
    print(f"[*] Input File:      {input_file_path}")
    print(f"[*] Output Target:   {output_csv_path}")
    print(f"[*] Chunk Size:      {chunk_size:,} rows per iteration")
    print("[*] CSV decoding:    encoding_errors='replace' (malformed bytes are "
          "replaced with U+FFFD instead of aborting ingestion).")
    if max_records:
        print(f"[*] Record Limit:    {max_records:,} articles")
    print(f"[*] Deduplication:   {'ON (--dedup)' if dedup else 'OFF (default)'}")

    # Reset the per-run imputation counters so repeated CLI invocations in one
    # interpreter cannot contaminate the sidecar report.
    _DATE_IMPUTATION_STATS["imputed_rows"] = 0
    _DATE_IMPUTATION_STATS["total_rows"] = 0

    if not os.path.exists(input_file_path):
        print(f"\n[!] Input file not found: {input_file_path}")
        print("[*] To use real Kaggle data:")
        print("    1. Download 'All the News' (143k or 2.7M articles) from Kaggle.")
        print("    2. Place the CSV file into 'data/' (e.g. data/all-the-news.csv).")
        print(f"    3. Run: python src/ingest_kaggle_real.py --input {input_file_path}")
        return False

    os.makedirs(os.path.dirname(output_csv_path) or ".", exist_ok=True)
    total_written = 0
    first_chunk = True
    dropped_duplicates = 0
    seen_ids = set()
    seen_urls = set()

    try:
        reader = pd.read_csv(input_file_path, chunksize=chunk_size, encoding="utf-8", encoding_errors="replace")
        for chunk_idx, chunk in enumerate(reader):
            df_clean = map_kaggle_row_chunk(chunk, chunk_idx)
            if df_clean.empty:
                continue

            # Optional --dedup: drop repeats within this chunk and against all
            # previously written chunks, keyed on article_id/url.
            if dedup:
                before = len(df_clean)
                df_clean = df_clean.drop_duplicates(subset="article_id", keep="first")
                df_clean = df_clean.drop_duplicates(subset="url", keep="first")
                df_clean = df_clean[~df_clean["article_id"].isin(seen_ids)]
                df_clean = df_clean[~df_clean["url"].isin(seen_urls)]
                dropped_duplicates += before - len(df_clean)
                seen_ids.update(df_clean["article_id"].astype(str))
                seen_urls.update(df_clean["url"].astype(str))
                if df_clean.empty:
                    continue

            # Check if limit reached
            if max_records and (total_written + len(df_clean)) > max_records:
                df_clean = df_clean.head(max_records - total_written)

            # Append to target CSV
            mode = "w" if first_chunk else "a"
            header = first_chunk
            df_clean.to_csv(output_csv_path, mode=mode, header=header, index=False, encoding="utf-8")
            
            total_written += len(df_clean)
            first_chunk = False
            print(f"    - Processed Chunk {chunk_idx + 1}: Ingested {total_written:,} articles...")

            if max_records and total_written >= max_records:
                print(f"[*] Reached configured record limit of {max_records:,} articles.")
                break

        if dedup:
            print(f"[*] --dedup removed {dropped_duplicates:,} duplicate article_id/url rows.")
        write_date_imputation_report()
        print(f"\n[+] Ingestion Complete! Total adapted records: {total_written:,}")
        print(f"[+] Output ready at: {output_csv_path}")
        print("[*] You can now re-run 'python run_all.py' to process the real Kaggle dataset.")
        return True

    except Exception as e:
        print(f"[!] Error during Kaggle ingestion: {e}")
        write_date_imputation_report()
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest and adapt Kaggle 'All the News' CSV into pipeline schema.")
    parser.add_argument("--input", "-i", type=str, default="data/all-the-news.csv", help="Path to raw Kaggle CSV file")
    parser.add_argument("--output", "-o", type=str, default="data/raw_news_articles.csv", help="Target output CSV path")
    # "--max-rows" / "--chunk-size" are the flag spellings documented in README Method 7.
    parser.add_argument("--limit", "-l", "--max-rows", dest="limit", type=int, default=None, help="Maximum number of articles to ingest")
    parser.add_argument("--chunksize", "-c", "--chunk-size", dest="chunksize", type=int, default=10000, help="Chunk size for streaming processing")
    parser.add_argument("--dedup", action="store_true",
                        help="Drop duplicate article_id/url rows before writing (default: off).")

    args = parser.parse_args()
    ingest_kaggle_file(args.input, args.output, args.chunksize, args.limit, dedup=args.dedup)
