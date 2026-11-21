# Apache ZooKeeper — Real Production Cluster Commands

Project : News Article Analysis and Topic Extraction
Owner   : Member 2 — Priya Sharma (Hadoop & Cluster Specialist)

## 1. Start / stop a ZooKeeper node

```bash
# Linux
zkServer.sh start
zkServer.sh status
zkServer.sh stop

# Windows
zkServer.cmd start
zkServer.cmd status
```

## 2. Connect with the command-line client

```bash
zkCli.cmd -server localhost:2181
zkCli.sh  -server localhost:2181
```

## 3. Create the pipeline znodes

```bash
create /news-pipeline ""
create /news-pipeline/config "lda_k=6"
create /news-pipeline/election ""
create -e -s /news-pipeline/election/worker- "session=worker-1"
```

## 4. Read them back

```bash
get /news-pipeline/config
ls /news-pipeline/election
stat /news-pipeline/election
ls -w /news-pipeline/config      # set a watch, then change the data elsewhere
```

## 5. Update / clean up

```bash
set /news-pipeline/config "lda_k=8"
delete /news-pipeline/config
rmr /news-pipeline               # recursive delete (deprecated; use deleteall)
```

## 6. The quorum rule (2n+1)

A ZooKeeper ensemble of `2n+1` nodes tolerates the failure of `n` nodes while still
forming a quorum (majority). A 3-node ensemble (n=1) survives 1 node loss; a 5-node
ensemble (n=2) survives 2. Always deploy an ODD number of nodes so ties are impossible:

```
server.1=zk-node-1:2888:3888
server.2=zk-node-2:2888:3888
server.3=zk-node-3:2888:3888   # 3 = 2*1 + 1
```

## Local simulation result on this workstation
- Znodes created     : 11
- Ephemeral znodes   : 6
- Sessions            : 4
- Leader elected      : /news-pipeline/election/worker-0000000001
- Watches fired       : 3
- Config version      : 1
