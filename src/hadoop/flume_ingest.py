"""
Apache Flume Streaming Ingestion Layer (Streaming -> HDFS)
==========================================================
Project : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache Flume — reliable streaming ingestion of news events into HDFS
Owner   : Member 5 — Mohamed Ibrahim (NoSQL & Data Integration Specialist)

WHAT THIS MODULE DOES
---------------------
A production news platform receives a continuous feed of article events (wire feeds,
editorial CMS webhooks, API streamers). Apache Flume ingests those events reliably
into HDFS. The canonical Flume agent has three layers:

    SOURCE  (spooldir / taildir / kafka)
        |  produces events
    CHANNEL (memory / file / kafka)   <- transactional buffer
        |  delivers events
    SINK    (hdfs / hbase / kafka)

This module is DUAL-MODE:

    1. LOCAL SIMULATION — real news articles are turned into JSON-line events and
       written into a REAL `spool/` directory. An in-process `MemoryChannel` class
       (with genuine put / take / commit / rollback transaction semantics and a
       capacity limit) is driven by a source loop and drained by a sink loop that
       uploads each batch into the shared `LocalHDFS` simulation under
       /user/news/stream/YYYYMMDD/. Per-event bytes and throughput are measured.
    2. REAL CLUSTER ARTIFACTS — a genuine `agent_news.properties` Flume agent config
       (spooldir source + timestamp/regex_filter interceptors, memory channel, HDFS
       sink) and `real_cluster_commands.md` with the authentic `flume-ng agent`
       launch command.

The public entry point is `run_flume_ingestion(...)`.
"""

# ---------------------------------------------------------------------------
# Path bootstrap: runnable as `python src/hadoop/flume_ingest.py` and importable
# as `from src.hadoop.flume_ingest import run_flume_ingestion`.
# ---------------------------------------------------------------------------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.hadoop.hdfs_client import LocalHDFS

# --- Standard library only -------------------------------------------------
import csv          # read the raw article CSV
import json         # build JSON-line events + summary
import time         # measure streaming throughput
import datetime     # build the hdfs path %Y%m%d date stamp
from collections import deque   # the channel's in-memory FIFO queue

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _resolve(path: str) -> str:
    """Return an absolute path, resolving relative paths against the project root."""
    return path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)


def _rel(path: str) -> str:
    """Return a project-relative, forward-slash path."""
    return os.path.relpath(os.path.abspath(path), PROJECT_ROOT).replace("\\", "/")


class ChannelFull(Exception):
    """Raised by MemoryChannel.put when the channel is at capacity (Flume ChannelException)."""


class Event:
    """
    A minimal Flume event: an opaque byte body plus a header map (e.g. the
    timestamp interceptor writes the `timestamp` header, the regex_filter
    interceptor reads the body to decide whether to keep the event).
    """

    __slots__ = ("body", "headers")

    def __init__(self, body: bytes, headers: dict = None):
        self.body = body                       # the raw payload delivered to the sink
        self.headers = headers or {}           # key/value metadata carried alongside

    def __len__(self) -> int:
        """Byte size of the event body (used for throughput accounting)."""
        return len(self.body)


class MemoryChannel:
    """
    A faithful in-process model of Flume's Memory Channel.

    Semantics reproduced
    --------------------
    * `put(event)`   stages an event inside the current PUT transaction buffer.
    * `take()`       moves an event from the committed queue into the TAKE buffer.
    * `commit()`     atomically promotes staged puts into the queue AND confirms
                     staged takes (removing them permanently).
    * `rollback()`   discards staged puts and returns staged takes to the queue,
                     which is exactly how Flume recovers an aborted transaction.
    * `capacity`     hard upper bound on the number of events in flight.
    * `transaction_capacity`  maximum events per single transaction.
    """

    def __init__(self, capacity: int = 1000, transaction_capacity: int = 100,
                 name: str = "a1.channels.c1"):
        self.name = name
        self.capacity = int(capacity)
        self.transaction_capacity = int(transaction_capacity)
        # committed events waiting to be delivered
        self.queue = deque()
        # staged (uncommitted) buffers for the two transaction sides
        self.put_buffer = []
        self.take_buffer = []
        # instrumentation
        self.put_count = 0
        self.take_count = 0
        self.rollback_count = 0

    # -- capacity bookkeeping -------------------------------------------------
    @property
    def size(self) -> int:
        """Total events logically resident in the channel (incl. staged)."""
        return len(self.queue) + len(self.put_buffer) + len(self.take_buffer)

    # -- PUT side -------------------------------------------------------------
    def put(self, event: Event) -> None:
        """Stage an event; raise ChannelFull when the capacity would be exceeded."""
        if self.size >= self.capacity:
            raise ChannelFull(f"channel {self.name} at capacity ({self.capacity})")
        self.put_buffer.append(event)

    # -- TAKE side ------------------------------------------------------------
    def take(self):
        """Move a committed event into the take buffer; return None when empty."""
        if not self.queue:
            return None
        event = self.queue.popleft()
        self.take_buffer.append(event)
        return event

    # -- transaction control --------------------------------------------------
    def commit(self) -> None:
        """Atomically confirm the current PUT and TAKE transactions."""
        # 1) promote every staged put into the committed queue
        for event in self.put_buffer:
            self.queue.append(event)
        self.put_count += len(self.put_buffer)
        self.put_buffer.clear()
        # 2) confirm the takes (they are now permanently removed)
        self.take_count += len(self.take_buffer)
        self.take_buffer.clear()

    def rollback(self) -> None:
        """Abort the current transaction, restoring the channel to its prior state."""
        # staged puts are simply discarded (never happened)
        self.put_buffer.clear()
        # staged takes are pushed back to the FRONT of the queue, in order
        for event in reversed(self.take_buffer):
            self.queue.appendleft(event)
        self.take_buffer.clear()
        self.rollback_count += 1


def _build_agent_config(config_path: str, spool_dir: str, hdfs_path: str,
                        capacity: int, tx_capacity: int, batch_size: int) -> str:
    """
    Write a genuine Apache Flume agent configuration (agent_news.properties).
    Property keys are the real ones Flume reads for a spooldir -> memory -> hdfs
    topology with a timestamp and a regex_filter interceptor.
    """
    rel_spool = _rel(spool_dir)
    text = f"""# =============================================================================
# agent_news.properties — Apache Flume agent: news event stream -> HDFS
# Project : News Article Analysis and Topic Extraction (Big Data Architecture)
# Owner   : Member 5 — Mohamed Ibrahim (NoSQL & Data Integration Specialist)
#
# Topology:  spooldir source  ==>  memory channel  ==>  hdfs sink
# Start this agent on the edge node with:
#   flume-ng agent --conf ./conf --conf-file agent_news.properties \\
#                  --name a1 -Dflume.root.logger=INFO,console
# =============================================================================

# --- Agent name and component wiring -----------------------------------------
a1.sources = r1
a1.channels = c1
a1.sinks = k1

# =============================================================================
# SOURCE: Spooling Directory Source — watches a directory for new files and
# emits one event per line. Completed files are renamed with a suffix/compressed.
# =============================================================================
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = {rel_spool}
a1.sources.r1.fileSuffix = .COMPLETED
a1.sources.r1.deletePolicy = never
a1.sources.r1.ignorePattern = ^(.*\\.tmp)$
a1.sources.r1.batchSize = {batch_size}
a1.sources.r1.inputCharset = UTF-8
a1.sources.r1.channels = c1

# --- Interceptors: timestamp + regex_filter ----------------------------------
a1.sources.r1.interceptors = i1 i2
# i1: insert the ingest-time epoch-millis into the event headers
a1.sources.r1.interceptors.i1.type = timestamp
# i2: keep only genuine JSON article events (drop garbage/header lines)
a1.sources.r1.interceptors.i2.type = regex_filter
a1.sources.r1.interceptors.i2.regex = ^\\{{.*"article_id".*\\}}$
a1.sources.r1.interceptors.i2.excludeEvents = false

# =============================================================================
# CHANNEL: Memory Channel — a fast, transactional in-memory buffer.
# =============================================================================
a1.channels.c1.type = memory
a1.channels.c1.capacity = {capacity}
a1.channels.c1.transactionCapacity = {tx_capacity}
a1.channels.c1.byteCapacityBufferPercentage = 20
a1.channels.c1.byteCapacity = 52428800
a1.channels.c1.keep-alive = 3

# =============================================================================
# SINK: HDFS Sink — writes events under a date-partitioned path.
# The %Y%m%d tokens are resolved per event from the timestamp header.
# =============================================================================
a1.sinks.k1.type = hdfs
a1.sinks.k1.channel = c1
a1.sinks.k1.hdfs.path = {hdfs_path}
a1.sinks.k1.hdfs.filePrefix = FlumeData
a1.sinks.k1.hdfs.fileSuffix = .json
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.writeFormat = Text
a1.sinks.k1.hdfs.rollInterval = 30
a1.sinks.k1.hdfs.rollSize = 0
a1.sinks.k1.hdfs.rollCount = {batch_size}
a1.sinks.k1.hdfs.batchSize = {batch_size}
a1.sinks.k1.hdfs.callTimeout = 60000
a1.sinks.k1.hdfs.useLocalTimeStamp = true
a1.sinks.k1.hdfs.round = true
a1.sinks.k1.hdfs.roundValue = 10
a1.sinks.k1.hdfs.roundUnit = minute
a1.sinks.k1.hdfs.minBlockReplicas = 1
"""
    with open(config_path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return text


def _write_real_cluster_commands(path: str, stats: dict) -> None:
    """Write the authentic Flume cluster runbook (real_cluster_commands.md)."""
    md = f"""# Apache Flume — Real Production Cluster Commands

Project : News Article Analysis and Topic Extraction
Owner   : Member 5 — Mohamed Ibrahim (NoSQL & Data Integration Specialist)

## 1. Start the agent on an edge node

```bash
export FLUME_HOME=/opt/cloudera/parcels/FLUME
flume-ng agent \\
  --conf ./conf \\
  --conf-file agent_news.properties \\
  --name a1 \\
  -Dflume.root.logger=INFO,console
```

Run it detached under a supervisor (systemd) in production:

```bash
nohup flume-ng agent --conf ./conf --conf-file agent_news.properties \\
      --name a1 -Dflume.root.logger=INFO,LOGFILE > flume-a1.log 2>&1 &
```

## 2. Key configuration knobs

- `a1.sources.r1.spoolDir` — the drop directory watched for new files.
  Files are picked up automatically and renamed with `.COMPLETED`.
- `a1.channels.c1.capacity` — the maximum number of events buffered in memory.
- `a1.channels.c1.transactionCapacity` — events committed per transaction.
- `a1.sinks.k1.hdfs.path` — HDFS destination; `%Y%m%d` is the date partition.
- `a1.sinks.k1.hdfs.fileType = DataStream` — writes raw event bodies (no Avro wrapper).
- `a1.sinks.k1.hdfs.rollCount` / `rollInterval` / `rollSize` — file roll triggers.
- `a1.sinks.k1.hdfs.batchSize` — events buffered before an HDFS flush.
- `a1.sinks.k1.hdfs.callTimeout` — HDFS RPC timeout in milliseconds.

## 3. Verify the ingested data on the cluster

```bash
hdfs dfs -ls -h /user/news/stream/$(date +%Y%m%d)
hdfs dfs -cat /user/news/stream/$(date +%Y%m%d)/FlumeData.* | head
hdfs dfs -du -s -h /user/news/stream
```

## 4. Stream the same feed through Kafka (optional upgrade)

```bash
# source becomes a Kafka source instead of spooldir
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.kafka.bootstrap.servers = kafka-1:9092
a1.sources.r1.kafka.topics = news-events
a1.sources.r1.kafka.consumer.group.id = flume-news
```

## Local simulation result on this workstation
- Events ingested : {stats.get('events_ingested', 0)}
- Bytes ingested  : {stats.get('bytes_ingested', 0)}
- HDFS stream dir : {stats.get('hdfs_stream_paths', ['<none>'])[0] if stats.get('hdfs_stream_paths') else '<none>'}
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(md)


def run_flume_ingestion(input_csv="data/raw_news_articles.csv",
                        out_dir="data/hadoop_ecosystem/flume",
                        hdfs_root="data/hadoop_ecosystem/hdfs",
                        num_events=500) -> dict:
    """
    Execute the full Flume streaming-ingestion demonstration.

    Returns the mandated contract dict with keys:
        agent_config, events_ingested, bytes_ingested, hdfs_stream_paths,
        events_per_second, channel_capacity, out_dir
    """
    print(f"[Flume] Starting Flume streaming ingestion simulation ({num_events} events) ...")
    out_dir = _resolve(out_dir)
    spool_dir = os.path.join(out_dir, "spool")
    os.makedirs(spool_dir, exist_ok=True)

    # --- Channel / sink tuning (mirrors the generated agent config) ----------
    channel_capacity = max(1000, num_events + 10)
    transaction_capacity = 100
    hdfs_batch_size = 100

    # --- STEP 1: convert real articles into JSON-line spool files ------------
    csv_path = _resolve(input_csv)
    with open(csv_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        articles = []
        for i, row in enumerate(reader):
            if i >= num_events:
                break
            articles.append(row)

    # Write the events into the spool directory, batched into several files so the
    # spooldir source has multiple files to pick up (Flume uses the file name for
    # de-duplication, hence the zero-padded suffix).
    events_per_file = 100
    spool_files = []
    for batch_index, start in enumerate(range(0, len(articles), events_per_file), start=1):
        batch = articles[start:start + events_per_file]
        spool_path = os.path.join(spool_dir, f"news_events_{batch_index:04d}.json")
        with open(spool_path, "w", encoding="utf-8") as sfh:
            for row in batch:
                # Each line is one event body: a compact JSON document.
                event_doc = {
                    "article_id": row.get("article_id"),
                    "title": row.get("title"),
                    "publication": row.get("publication"),
                    "category": row.get("category"),
                    "date": row.get("date"),
                    "word_count": row.get("word_count"),
                }
                sfh.write(json.dumps(event_doc, ensure_ascii=False) + "\n")
        spool_files.append(spool_path)
    print(f"[Flume] Wrote {len(articles)} events into {len(spool_files)} spool file(s) at {_rel(spool_dir)}")

    # --- STEP 2: run the source -> memory-channel -> hdfs-sink pipeline ------
    channel = MemoryChannel(capacity=channel_capacity,
                            transaction_capacity=transaction_capacity)
    stamp = datetime.datetime.now().strftime("%Y%m%d")
    hdfs_dir = f"/user/news/stream/{stamp}"

    # --- The shared HDFS simulation plays the role of the HDFS cluster -------
    fs = LocalHDFS(root=_resolve(hdfs_root), block_size_mb=1, replication=3,
                   num_datanodes=3, verbose=False)
    fs.rm(hdfs_dir, recursive=True)   # clean today's partition so the run is reproducible
    fs.mkdirs(hdfs_dir)

    started = time.time()
    events_ingested = 0
    bytes_ingested = 0
    hdfs_stream_paths = []

    # ---- SOURCE side: read each spool file line-by-line, put into channel ----
    for spool_path in spool_files:
        with open(spool_path, "r", encoding="utf-8") as sfh:
            for line in sfh:
                line = line.rstrip("\n")
                if not line:
                    continue
                # The timestamp interceptor stamps the header with "now".
                stamp_ms = str(int(time.time() * 1000))
                # The regex_filter interceptor drops non-JSON garbage (all lines pass here).
                if '"article_id"' not in line:
                    continue
                event = Event(line.encode("utf-8"), headers={"timestamp": stamp_ms})
                # source -> channel ; commit every `transaction_capacity` events
                channel.put(event)
                if len(channel.put_buffer) >= channel.transaction_capacity:
                    channel.commit()
    channel.commit()  # flush the final partial transaction
    print(f"[Flume] Source committed {channel.put_count} events into memory channel "
          f"(capacity={channel_capacity}, txCapacity={transaction_capacity})")

    # ---- SINK side: drain the channel and write batches into HDFS ------------
    batch = []
    file_index = 0
    while True:
        event = channel.take()
        if event is None:
            break
        batch.append(event)
        events_ingested += 1
        bytes_ingested += len(event)
        # When a full HDFS batch is ready, flush it as one sink output file.
        if len(batch) >= hdfs_batch_size:
            payload = b"".join((ev.body + b"\n") for ev in batch)
            hdfs_file = f"{hdfs_dir}/FlumeData.{stamp}.{file_index:05d}.json"
            fs.put_bytes(payload, hdfs_file, source_label=f"flume-sink-{file_index}")
            hdfs_stream_paths.append(hdfs_file)
            channel.commit()   # confirm the takes for the flushed events
            batch = []
            file_index += 1
    # Flush the trailing partial batch.
    if batch:
        payload = b"".join((ev.body + b"\n") for ev in batch)
        hdfs_file = f"{hdfs_dir}/FlumeData.{stamp}.{file_index:05d}.json"
        fs.put_bytes(payload, hdfs_file, source_label=f"flume-sink-{file_index}")
        hdfs_stream_paths.append(hdfs_file)
        channel.commit()
    elapsed = max(time.time() - started, 1e-9)
    events_per_second = round(events_ingested / elapsed, 2)
    print(f"[Flume] Sink delivered {channel.take_count} events into {len(hdfs_stream_paths)} "
          f"HDFS file(s) under {hdfs_dir}")

    # --- Demonstrate rollback semantics on a throw-away channel --------------
    demo = MemoryChannel(capacity=10, transaction_capacity=5)
    demo.put(Event(b'{"article_id":"ROLLBACK-DEMO"}'))
    demo.commit()                 # 1 event committed
    taken = demo.take()           # moved into the take buffer
    demo.rollback()               # abort the take -> event survives
    rollback_restored = demo.size == 1 and taken is not None
    print(f"[Flume] Channel rollback check: {'restored event successfully' if rollback_restored else 'FAILED'}")

    # --- STEP 3: write the agent config + summary + real commands ------------
    agent_config_path = os.path.join(out_dir, "agent_news.properties")
    _build_agent_config(agent_config_path, spool_dir, "/user/news/stream/%Y%m%d",
                        channel_capacity, transaction_capacity, hdfs_batch_size)

    summary = {
        "agent_config": _rel(agent_config_path),
        "events_ingested": events_ingested,
        "bytes_ingested": bytes_ingested,
        "hdfs_stream_paths": hdfs_stream_paths,
        "events_per_second": events_per_second,
        "channel_capacity": channel_capacity,
        "out_dir": _rel(out_dir),
    }
    with open(os.path.join(out_dir, "flume_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    _write_real_cluster_commands(os.path.join(out_dir, "real_cluster_commands.md"), summary)

    print(f"[Flume] Done: {events_ingested} events, {bytes_ingested} bytes, "
          f"{events_per_second} events/s.")
    return summary


if __name__ == "__main__":
    # Run the component stand-alone and print a completion summary.
    result = run_flume_ingestion()
    print("[Flume] Completion summary:")
    print(json.dumps(result, indent=2, default=str))
