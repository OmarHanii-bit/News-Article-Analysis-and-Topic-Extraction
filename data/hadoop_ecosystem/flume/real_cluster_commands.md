# Apache Flume — Real Production Cluster Commands

Project : News Article Analysis and Topic Extraction
Owner   : Member 5 — Mohamed Ibrahim (NoSQL & Data Integration Specialist)

## 1. Start the agent on an edge node

```bash
export FLUME_HOME=/opt/cloudera/parcels/FLUME
flume-ng agent \
  --conf ./conf \
  --conf-file agent_news.properties \
  --name a1 \
  -Dflume.root.logger=INFO,console
```

Run it detached under a supervisor (systemd) in production:

```bash
nohup flume-ng agent --conf ./conf --conf-file agent_news.properties \
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
- Events ingested : 500
- Bytes ingested  : 103790
- HDFS stream dir : /user/news/stream/20261125/FlumeData.20261125.00000.json
