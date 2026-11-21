#!/usr/bin/env python
"""
Hadoop Streaming Reducer: Distributed Term & N-Gram Frequency Aggregation
========================================================================
Component: MapReduce Aggregation & Frequency Summarization Layer
Authors: Big Data Engineering Team (Member 2: Hadoop/HDFS Pipeline Lead)

Description:
------------
This script operates as a standard Hadoop Streaming Reducer.
Input:  Sorted key-value pairs (term \t count) streamed from STDIN.
Output: Aggregated totals (term \t total_frequency) streamed to STDOUT.
Features:
- Handles standard Hadoop Streaming sorted partition output
- State-machine key transition tracking
- Memory-efficient streaming summation without unbounded in-memory dictionaries
- Filters out low-frequency noise (minimum frequency threshold)
"""

import sys

def process_stream(min_frequency_threshold=5):
    """
    Consumes sorted (key, value) pairs from sys.stdin, aggregates counts for
    identical keys, and emits finalized counts to sys.stdout once a key change occurs.
    
    Parameters:
    -----------
    min_frequency_threshold : int
        Minimum threshold required to retain a token in the vocabulary.
        Discards rare singleton spelling errors or low-signal noise.
    """
    # Track the active key being accumulated across the stream
    current_key = None
    
    # Running sum accumulator for the active key
    current_count = 0

    # Stream through lines provided by Hadoop Shuffle & Sort
    for line in sys.stdin:
        # Strip trailing newlines and whitespace
        clean_line = line.strip()
        
        # Guard clause: skip empty lines
        if not clean_line:
            continue

        # Parse key and count separated by tab
        try:
            key, count_str = clean_line.split("\t", 1)
            count = int(count_str)
        except ValueError:
            # Skip improperly formatted lines defensively
            continue

        # Check if the incoming key matches the active accumulated key
        if current_key == key:
            # Increment running tally
            current_count += count
        else:
            # If transitioning to a new key, flush the completed previous key
            if current_key is not None:
                # Apply frequency threshold filtering
                if current_count >= min_frequency_threshold:
                    sys.stdout.write(f"{current_key}\t{current_count}\n")
            
            # Reset state for the newly encountered key
            current_key = key
            current_count = count

    # Flush the final key lingering after stream completion
    if current_key is not None and current_count >= min_frequency_threshold:
        sys.stdout.write(f"{current_key}\t{current_count}\n")

if __name__ == "__main__":
    # Execute reduction stream processing
    process_stream(min_frequency_threshold=5)
