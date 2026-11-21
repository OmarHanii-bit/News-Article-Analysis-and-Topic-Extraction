#!/usr/bin/env python
"""
Hadoop Streaming Mapper: Distributed Token & N-Gram Extraction
=============================================================
Component: MapReduce Ingestion & Word Frequency Layer
Authors: Big Data Engineering Team (Member 2: Hadoop/HDFS Pipeline Lead)

Description:
------------
This script operates as a standard Hadoop Streaming Mapper.
Input:  Lines of CSV data streamed from STDIN (HDFS chunks split across Map tasks).
Output: Tab-delimited key-value pairs (term \t 1) streamed to STDOUT.
Features:
- Regex-based text cleaning and tokenization
- Stopwords filtering
- Lowercasing and punctuation stripping
- Bigram and unigram candidate extraction
"""

import sys
import re
import csv

# Define a standard English stopword list for distributed filtering without heavy external library dependencies
STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", 
    "arent", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", 
    "but", "by", "cant", "cannot", "could", "couldnt", "did", "didnt", "do", "does", "doesnt", 
    "doing", "dont", "down", "during", "each", "few", "for", "from", "further", "had", "hadnt", 
    "has", "hasnt", "have", "havent", "having", "he", "hed", "hell", "hes", "her", "here", 
    "heres", "hers", "herself", "him", "himself", "his", "how", "hows", "i", "id", "ill", "im", 
    "ive", "if", "in", "into", "is", "isnt", "it", "its", "itself", "lets", "me", "more", "most", 
    "mustnt", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", 
    "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shant", "she", "shed", 
    "shell", "shes", "should", "shouldnt", "so", "some", "such", "than", "that", "thats", "the", 
    "their", "theirs", "them", "themselves", "then", "there", "theres", "these", "they", "theyd", 
    "theyll", "theyre", "theyve", "this", "those", "through", "to", "too", "under", "until", 
    "up", "very", "was", "wasnt", "we", "wed", "well", "were", "weve", "werent", "what", 
    "whats", "when", "whens", "where", "wheres", "which", "while", "who", "whos", "whom", 
    "why", "whys", "with", "wont", "would", "wouldnt", "you", "youd", "youll", "youre", 
    "youve", "your", "yours", "yourself", "yourselves", "said", "also", "would", "could", "one",
    "two", "new", "first", "last", "year", "years", "time", "day", "days", "reuters", "bloomberg"
}

# Precompile regular expressions for maximum streaming execution performance
REGEX_CLEAN_HTML = re.compile(r"<[^>]+>")             # Match and purge HTML markup tags
REGEX_CLEAN_URL = re.compile(r"http[s]?://\S+")        # Match and purge HTTP/HTTPS web links
# Keep this EXACTLY in sync with the Spark cleaning regex `&[a-z0-9#]+;`
# (src/spark_pipeline.py / src/lda_topic_selection.py): numeric and '#'-coded
# entities must be purged too, otherwise MapReduce and Spark term counts diverge.
REGEX_CLEAN_ENTITIES = re.compile(r"&[a-z0-9#]+;")    # Match and purge XML/HTML/numeric entities
REGEX_WORD_TOKENS = re.compile(r"\b[a-zA-Z]{3,20}\b") # Extract clean alphabetic words (length 3 to 20)

def process_stream():
    """
    Reads stream lines from sys.stdin, parses CSV rows, extracts cleaned article content,
    generates token counts, and outputs key-value pairs formatted for Hadoop sort/shuffle.
    """
    # Use csv.reader to handle multi-line strings, quoted commas, and escape sequences properly
    reader = csv.reader(sys.stdin)
    
    # Track line index to identify and bypass header row
    is_header = True

    for row in reader:
        # Guard clause: ensure valid row content
        if not row:
            continue
            
        # Detect and bypass CSV header line
        if is_header:
            is_header = False
            # Check if current line contains known column headers
            if "article_id" in row or "content" in row or "title" in row:
                continue

        # Extract title and body content safely from expected CSV indices
        # Typical schema: [article_id, title, author, date, year, month, publication, category, sub_topic, content, word_count, url]
        title = ""
        content = ""
        
        if len(row) >= 10:
            title = row[1]
            content = row[9]
        elif len(row) >= 2:
            title = row[0]
            content = row[1]
        else:
            content = row[0]

        # Combine title and article content for comprehensive text feature representation
        full_text = f"{title} {content}"

        # Step 1: Strip HTML tags, URL strings, and entities
        cleaned_text = REGEX_CLEAN_HTML.sub(" ", full_text)
        cleaned_text = REGEX_CLEAN_URL.sub(" ", cleaned_text)
        cleaned_text = REGEX_CLEAN_ENTITIES.sub(" ", cleaned_text)

        # Step 2: Convert to lowercase for uniform case aggregation
        lowercased_text = cleaned_text.lower()

        # Step 3: Extract valid alphabetic word tokens
        tokens = REGEX_WORD_TOKENS.findall(lowercased_text)

        # Step 4: Filter tokens against the stopwords set
        filtered_tokens = [tok for tok in tokens if tok not in STOPWORDS]

        # Step 5: Emit unigram key-value pairs to STDOUT
        for tok in filtered_tokens:
            # Emit key \t value for Hadoop Shuffle & Sort
            sys.stdout.write(f"{tok}\t1\n")

        # Step 6: Emit bigram key-value pairs (two-word phrases) for phrase mining
        for i in range(len(filtered_tokens) - 1):
            bigram = f"{filtered_tokens[i]}_{filtered_tokens[i+1]}"
            sys.stdout.write(f"BI_{bigram}\t1\n")

if __name__ == "__main__":
    # Execute stream processing logic
    process_stream()
