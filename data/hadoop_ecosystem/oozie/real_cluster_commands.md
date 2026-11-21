# Apache Oozie — Real Production Cluster Commands

Project : News Article Analysis and Topic Extraction
Owner   : Member 3 — Marcus Chen (Distributed Warehouse Engineer)

## 1. Validate the workflow XML before submitting

```bash
oozie validate workflow.xml
```

## 2. Upload the workflow definition to HDFS (the Oozie app path)

```bash
hdfs dfs -mkdir -p /user/news/oozie/news_pipeline
hdfs dfs -put workflow.xml coordinator.xml job.properties /user/news/oozie/news_pipeline/
```

## 3. Run the workflow on the cluster

```bash
oozie job -oozie http://oozie-host:11000/oozie -config job.properties -run
```

## 4. Submit the coordinator (runs the workflow on its schedule)

```bash
oozie job -oozie http://oozie-host:11000/oozie -config job.properties \
          -submit -Doozie.coord.application.path=$oozie.coord.application.path
oozie job -oozie http://oozie-host:11000/oozie -config job.properties -start
```

## 5. Inspect the running jobs

```bash
oozie jobs -oozie http://oozie-host:11000/oozie -filter status=RUNNING
oozie job -oozie http://oozie-host:11000/oozie -info <job-id>
oozie job -oozie http://oozie-host:11000/oozie -log <job-id>
```

## Local simulation result on this workstation
- Workflow actions : sqoop_import, pig_transform, hive_analytics, spark_topics, shell_verify
- Actions executed : 5
- Actions skipped  : 0
- Final status     : SUCCEEDED
