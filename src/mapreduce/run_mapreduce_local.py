"""
MapReduce Local & Cluster Execution Runner
==========================================
Demonstrates how to run the Hadoop MapReduce job both:
1. Locally using Python streaming pipes (simulating Hadoop Shuffle/Sort)
2. In a real Hadoop cluster using 'hadoop jar hadoop-streaming.jar'

Component: Testing & Deployment Harness
Authors: Big Data Engineering Team (Member 2: Hadoop/HDFS Pipeline Lead)
"""

import sys
import os
import subprocess

def run_local_simulation(input_csv="data/raw_news_articles.csv", output_tsv="data/mapreduce_term_counts.tsv"):
    """
    Simulates Hadoop MapReduce streaming execution locally:
    cat input_csv | python mapper.py | sort | python reducer.py > output_tsv
    """
    print(f"[*] Starting local MapReduce simulation...")
    print(f"[*] Input: {input_csv}")
    print(f"[*] Target: {output_tsv}")

    if not os.path.exists(input_csv):
        print(f"[!] Input file not found: {input_csv}. Generating dataset first...")
        from src.generate_sample_data import generate_realistic_news_dataset
        generate_realistic_news_dataset(input_csv, num_records=2000)

    # Resolve paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mapper_path = os.path.join(base_dir, "mapreduce", "mapper.py")
    reducer_path = os.path.join(base_dir, "mapreduce", "reducer.py")

    # Read input file and run mapper
    print("[1/3] Executing Map phase (mapper.py)...")
    with open(input_csv, "r", encoding="utf-8", errors="ignore") as in_f:
        p_map = subprocess.Popen([sys.executable, mapper_path], stdin=in_f, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
        map_out, map_err = p_map.communicate()

    if p_map.returncode != 0:
        print(f"[!] Mapper error: {map_err}")
        return False

    print(f"[2/3] Simulating Hadoop Shuffle & Sort phase...")
    # Sort lines alphabetically by key
    lines = map_out.strip().splitlines()
    lines.sort()
    sorted_stream = "\n".join(lines) + "\n"

    print(f"[3/3] Executing Reduce phase (reducer.py)...")
    p_red = subprocess.Popen([sys.executable, reducer_path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
    red_out, red_err = p_red.communicate(input=sorted_stream)

    if p_red.returncode != 0:
        print(f"[!] Reducer error: {red_err}")
        return False

    os.makedirs(os.path.dirname(output_tsv), exist_ok=True)
    with open(output_tsv, "w", encoding="utf-8") as out_f:
        out_f.write(red_out)

    output_lines = red_out.strip().splitlines()
    print(f"[+] MapReduce completed successfully!")
    print(f"[+] Extracted {len(output_lines):,} unique vocabulary terms & n-grams.")
    print(f"[+] Sample top results:")
    for line in output_lines[:10]:
        print(f"    {line}")
    return True

def print_hadoop_cluster_command():
    """Prints the production Hadoop cluster streaming command for HDFS."""
    cmd = """
================================================================================
PRODUCTION HADOOP CLUSTER EXECUTION COMMAND (HDFS):
================================================================================
$ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -D mapreduce.job.name="News_Article_Term_Extraction" \\
    -D mapreduce.job.reduces=8 \\
    -files src/mapreduce/mapper.py,src/mapreduce/reducer.py \\
    -mapper "python3 mapper.py" \\
    -reducer "python3 reducer.py" \\
    -input /hdfs/data/news/raw_news_articles.csv \\
    -output /hdfs/data/news/output_term_frequencies
================================================================================
"""
    print(cmd)

if __name__ == "__main__":
    run_local_simulation()
    print_hadoop_cluster_command()
