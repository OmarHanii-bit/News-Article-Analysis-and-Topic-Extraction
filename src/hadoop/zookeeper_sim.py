"""
Apache ZooKeeper Coordination Layer (Distributed Coordination Services)
=======================================================================
Project : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache ZooKeeper — leader election, distributed config & locks
Owner   : Member 2 — Priya Sharma (Hadoop & Cluster Specialist)

WHAT THIS MODULE DOES
---------------------
A production Hadoop cluster relies on ZooKeeper for the coordination primitives that
keep distributed components consistent:

    * leader election        — exactly one NameNode/ResourceManager is active;
    * distributed configuration — every worker reads its config from a znode and
                                 receives a notification when it changes;
    * distributed locks      — mutual exclusion for a shared resource (e.g. the
                                 single writer of the topic-model output).

ZooKeeper stores all of this in a memory-resident tree of *znodes*. This module is
DUAL-MODE:

    1. LOCAL SIMULATION — `ZooKeeperSim` is a faithful, on-disk znode tree with real
       create modes (PERSISTENT / EPHEMERAL / PERSISTENT_SEQUENTIAL /
       EPHEMERAL_SEQUENTIAL), full `stat` metadata (czxid, mzxid, version, cversion,
       ephemeralOwner, dataLength, numChildren, pzxid), one-shot watches, a sequential
       `%010d` suffix, ephemeral-session cleanup and a transaction log. A real
       coordination demo elects a leader among three pipeline worker sessions, pushes a
       distributed config change to watching workers, and acquires a distributed lock.
    2. REAL CLUSTER ARTIFACTS — a genuine `zoo.cfg` (3-node ensemble with
       server.N=host:2888:3888 entries) and `real_cluster_commands.md` with the
       authentic `zkServer` / `zkCli` commands.

The public entry point is `run_coordination_demo(...)`.
"""

# ---------------------------------------------------------------------------
# Path bootstrap: runnable as `python src/hadoop/zookeeper_sim.py` and importable
# as `from src.hadoop.zookeeper_sim import ZooKeeperSim, run_coordination_demo`.
# ---------------------------------------------------------------------------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.hadoop.hdfs_client import LocalHDFS  # noqa: F401  (kept for ecosystem parity)

# --- Standard library only -------------------------------------------------
import json         # persist the znode tree + write summaries
import time         # zxid / clock metadata
import datetime     # audit timestamps

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _resolve(path: str) -> str:
    """Return an absolute path, resolving relative paths against the project root."""
    return path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)


def _rel(path: str) -> str:
    """Return a project-relative, forward-slash path."""
    return os.path.relpath(os.path.abspath(path), PROJECT_ROOT).replace("\\", "/")


# --- ZooKeeper exception types (mirroring the real CLI errors) -------------
class ZKError(Exception):
    """Base class for all ZooKeeper simulation errors."""


class NoNodeError(ZKError):
    """Raised when a path does not exist (KeeperException.NoNode)."""


class NodeExistsError(ZKError):
    """Raised when creating a node that already exists (KeeperException.NodeExists)."""


class NotEmptyError(ZKError):
    """Raised when deleting a node that still has children (KeeperException.NotEmpty)."""


class BadVersionError(ZKError):
    """Raised when a versioned write sees an unexpected version."""


class ZooKeeperSim:
    """
    A faithful single-machine simulation of a ZooKeeper ensemble's znode tree.

    The tree is persisted to `znodes.json` so state survives across runs, and every
    mutation is appended to `zk_transactions.log`. All coordinates (zxid, version,
    cversion, ephemeralOwner, watch counters) are maintained like the real server.
    """

    # Canonical create modes accepted by create().
    MODES = ("persistent", "ephemeral", "persistent_sequential", "ephemeral_sequential")

    def __init__(self, root_dir="data/hadoop_ecosystem/zookeeper", session_id=1000):
        # Absolute output directory so the module is CWD-independent.
        self.root_dir = _resolve(root_dir)
        os.makedirs(self.root_dir, exist_ok=True)
        self.znodes_path = os.path.join(self.root_dir, "znodes.json")
        self.tx_log_path = os.path.join(self.root_dir, "zk_transactions.log")

        # The session that owns ephemeral nodes created without an explicit owner.
        self.session_id = int(session_id)

        # Global transaction id counter, monotonic like the real ZAB zxid.
        self.zxid = 0
        # Per-parent sequence counters (used by the SEQUENTIAL create modes).
        self.sequence = {}
        # The znode tree: path -> node metadata dict.
        self.nodes = {}
        # In-memory one-shot watches: (path, type) -> [callbacks].
        self.watches = {}
        # Counters for the summary.
        self.created_count = 0
        self.ephemeral_created = 0
        self.watches_fired = 0

        # --- Load the persisted tree, or bootstrap an empty root -------------
        self._load()

    # ------------------------------------------------------------------ #
    # Persistence + transaction log
    # ------------------------------------------------------------------ #
    def _load(self):
        """Load the persisted tree; create a bare root '/' on first run."""
        if os.path.exists(self.znodes_path):
            with open(self.znodes_path, "r", encoding="utf-8") as fh:
                state = json.load(fh)
            self.zxid = state.get("zxid", 0)
            self.sequence = state.get("sequence", {})
            self.nodes = state.get("nodes", {})
        # Always guarantee the root znode exists.
        if "/" not in self.nodes:
            self.nodes["/"] = {
                "data": "", "ephemeral": False, "sequential": False, "owner": 0,
                "czxid": 0, "mzxid": 0, "pzxid": 0, "version": 0, "cversion": 0,
            }

    def _save(self):
        """Atomically persist the tree to `znodes.json`."""
        tmp = self.znodes_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"zxid": self.zxid, "sequence": self.sequence, "nodes": self.nodes},
                      fh, indent=2)
        os.replace(tmp, self.znodes_path)

    def _tx(self, op, path, detail=""):
        """Append a single line to the human-readable transaction log."""
        line = (f"{datetime.datetime.now().isoformat(timespec='milliseconds')} "
                f"zxid={self.zxid:06d} {op:<8} {path} {detail}".rstrip())
        with open(self.tx_log_path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    # ------------------------------------------------------------------ #
    # Path helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _norm(path: str) -> str:
        """Normalise a znode path so '/a/b/' and 'a/b' resolve identically."""
        path = "/" + str(path).strip("/")
        return path if path != "/" else "/"

    @staticmethod
    def _parent(path: str) -> str:
        """Return the parent path of a znode ('/a/b' -> '/a')."""
        parent = os.path.dirname(path.rstrip("/")) or "/"
        return parent

    @staticmethod
    def _basename(path: str) -> str:
        """Return the leaf name of a znode path."""
        return path.rstrip("/").rsplit("/", 1)[-1]

    def _child_paths(self, path: str):
        """Return the full paths of all immediate children of `path`."""
        path = self._norm(path)
        return [p for p in self.nodes if p != "/" and self._parent(p) == path]

    def _next_zxid(self) -> int:
        """Advance and return the global transaction id."""
        self.zxid += 1
        return self.zxid

    # ------------------------------------------------------------------ #
    # Watch mechanism
    # ------------------------------------------------------------------ #
    def add_watch(self, path: str, callback, watch_type: str = "data"):
        """
        Register a one-shot watch. `callback(path, event)` is invoked once when the
        watched znode (or its children) next changes, mirroring ZooKeeper semantics.
        Returns the number of watches currently registered for that key.
        """
        path = self._norm(path)
        self.watches.setdefault((path, watch_type), []).append(callback)
        return len(self.watches[(path, watch_type)])

    def trigger_watch(self, path: str, watch_type: str = "data", event: str = "NodeDataChanged"):
        """Fire (and clear) every watch registered for `path` of the given type."""
        path = self._norm(path)
        callbacks = self.watches.pop((path, watch_type), [])
        for cb in callbacks:
            cb(path, event)      # notify the watcher
            self.watches_fired += 1
        return len(callbacks)

    # ------------------------------------------------------------------ #
    # CRUD operations
    # ------------------------------------------------------------------ #
    def create(self, path: str, data: str = "", mode: str = "persistent", owner=None) -> str:
        """
        Create a znode and return its real path (including the sequence suffix for
        the SEQUENTIAL modes). Supported modes:
            persistent, ephemeral, persistent_sequential, ephemeral_sequential
        """
        mode = str(mode).lower()
        if mode not in self.MODES:
            raise ZKError(f"unsupported create mode: {mode}")
        path = self._norm(path)
        parent = self._parent(path)
        if parent not in self.nodes:
            raise NoNodeError(f"parent node does not exist: {parent}")

        sequential = mode.endswith("sequential")
        ephemeral = mode.startswith("ephemeral")
        if ephemeral:
            owner = self.session_id if owner is None else int(owner)
        else:
            owner = 0

        # --- Resolve the final path (append the %010d sequence for sequential) --
        if sequential:
            seq = self.sequence.get(parent, 0) + 1
            self.sequence[parent] = seq
            full_path = f"{path}{seq:010d}"
        else:
            full_path = path
            if full_path in self.nodes:
                raise NodeExistsError(f"node already exists: {full_path}")

        # --- Register the new znode -------------------------------------------
        zxid = self._next_zxid()
        self.nodes[full_path] = {
            "data": data,
            "ephemeral": ephemeral,
            "sequential": sequential,
            "owner": owner,
            "czxid": zxid, "mzxid": zxid, "pzxid": zxid,
            "version": 0, "cversion": 0,
        }
        # Creating a child bumps the parent's child version (cversion + pzxid).
        pnode = self.nodes[parent]
        pnode["cversion"] += 1
        pnode["pzxid"] = zxid

        self.created_count += 1
        if ephemeral:
            self.ephemeral_created += 1
        self._tx("CREATE", full_path,
                 f"mode={mode} owner={owner} bytes={len(data.encode('utf-8'))}")
        self._save()
        # Notify any child watch on the parent.
        self.trigger_watch(parent, "child", "NodeChildrenChanged")
        return full_path

    def get(self, path: str):
        """Return (data, stat) for a znode; raise NoNodeError if it is absent."""
        path = self._norm(path)
        if path not in self.nodes:
            raise NoNodeError(f"node does not exist: {path}")
        return self.nodes[path]["data"], self.stat(path)

    def set(self, path: str, data: str):
        """Overwrite a znode's data (bumping version + mzxid); return its new stat."""
        path = self._norm(path)
        if path not in self.nodes:
            raise NoNodeError(f"node does not exist: {path}")
        zxid = self._next_zxid()
        node = self.nodes[path]
        node["data"] = data
        node["version"] += 1
        node["mzxid"] = zxid
        self._tx("SET", path, f"version={node['version']} bytes={len(data.encode('utf-8'))}")
        self._save()
        # Notify data watches on this node.
        self.trigger_watch(path, "data", "NodeDataChanged")
        return self.stat(path)

    def delete(self, path: str, version=None):
        """Delete a leaf znode (no children allowed); optional version check."""
        path = self._norm(path)
        if path == "/":
            raise ZKError("cannot delete the root znode")
        if path not in self.nodes:
            raise NoNodeError(f"node does not exist: {path}")
        if self._child_paths(path):
            raise NotEmptyError(f"node still has children: {path}")
        node = self.nodes[path]
        if version is not None and int(version) != node["version"]:
            raise BadVersionError(f"version mismatch on {path}: expected {node['version']}")
        zxid = self._next_zxid()
        del self.nodes[path]
        parent = self._parent(path)
        if parent in self.nodes:
            self.nodes[parent]["cversion"] += 1
            self.nodes[parent]["pzxid"] = zxid
        self._tx("DELETE", path, f"version={node['version']}")
        self._save()
        self.trigger_watch(parent, "child", "NodeChildrenChanged")
        self.trigger_watch(path, "data", "NodeDeleted")
        return node

    def get_children(self, path: str):
        """Return the sorted leaf names of a znode's immediate children."""
        path = self._norm(path)
        if path not in self.nodes:
            raise NoNodeError(f"node does not exist: {path}")
        return sorted(self._basename(p) for p in self._child_paths(path))

    def exists(self, path: str):
        """Return a stat dict if the znode exists, otherwise None."""
        path = self._norm(path)
        return self.stat(path) if path in self.nodes else None

    def stat(self, path: str) -> dict:
        """Return the full Stat structure of a znode (like `stat <path>` in zkCli)."""
        path = self._norm(path)
        if path not in self.nodes:
            raise NoNodeError(f"node does not exist: {path}")
        node = self.nodes[path]
        return {
            "czxid": node["czxid"],
            "mzxid": node["mzxid"],
            "pzxid": node["pzxid"],
            "version": node["version"],
            "cversion": node["cversion"],
            "ephemeralOwner": node["owner"] if node["ephemeral"] else 0,
            "dataLength": len(str(node["data"]).encode("utf-8")),
            "numChildren": len(self._child_paths(path)),
        }

    # ------------------------------------------------------------------ #
    # Session lifecycle (ephemeral node cleanup)
    # ------------------------------------------------------------------ #
    def close_session(self, session_id: int, watch_fire: bool = True) -> int:
        """
        Close a client session and remove every ephemeral znode it owns, exactly as
        a real ZooKeeper server garbage-collects ephemeral nodes on timeout/close.
        Returns the number of ephemeral znodes deleted.
        """
        session_id = int(session_id)
        owned = [p for p, n in self.nodes.items() if n["ephemeral"] and n["owner"] == session_id]
        # A real session close is ONE transaction (one zxid) regardless of node count.
        self._next_zxid()
        # Delete deepest paths first so parents can be removed after their children.
        removed = 0
        for path in sorted(owned, key=len, reverse=True):
            if path in self.nodes:
                parent = self._parent(path)
                del self.nodes[path]
                if parent in self.nodes:
                    self.nodes[parent]["cversion"] += 1
                removed += 1
                self._tx("EXPIRE", path, f"session={session_id}")
        if removed:
            self._save()
            if watch_fire:
                # Fire child watches for each affected parent.
                for parent in {self._parent(p) for p in owned}:
                    self.trigger_watch(parent, "child", "NodeChildrenChanged")
        return removed

    # ------------------------------------------------------------------ #
    # Snapshot helpers
    # ------------------------------------------------------------------ #
    def build_tree(self, path: str = "/") -> dict:
        """Recursively build a readable nested dict of the znode tree."""
        path = self._norm(path)
        node = self.nodes[path]
        children = sorted(self._child_paths(path))
        return {
            "path": path,
            "data": node["data"],
            "ephemeral": node["ephemeral"],
            "owner": node["owner"],
            "version": node["version"],
            "children": [self.build_tree(c) for c in children],
        }

    def render_text_tree(self, path: str = "/", prefix: str = "") -> str:
        """Render an ASCII-art dump of the znode tree for human inspection."""
        path = self._norm(path)
        node = self.nodes[path]
        kind = "EPHEMERAL" if node["ephemeral"] else "PERSISTENT"
        label = path if path != "/" else "/"
        data = f" = {node['data']!r}" if node["data"] else ""
        lines = [f"{prefix}{label} [{kind}]{data}"]
        for child in sorted(self._child_paths(path)):
            lines.append(self.render_text_tree(child, prefix + "    "))
        return "\n".join(lines)


def _write_zoo_cfg(path: str, data_dir: str) -> None:
    """Write a genuine single-machine (3-node ensemble) ZooKeeper zoo.cfg."""
    text = f"""# =============================================================================
# zoo.cfg — Apache ZooKeeper ensemble configuration
# Project : News Article Analysis and Topic Extraction (Big Data Architecture)
# Owner   : Member 2 — Priya Sharma (Hadoop & Cluster Specialist)
#
# A ZooKeeper ensemble must have an ODD number of nodes (2n+1) so that it can form
# a quorum and tolerate the loss of n nodes. This cluster runs a 3-node ensemble.
# =============================================================================

# --- Time unit in milliseconds ------------------------------------------------
tickTime=2000

# --- Initialisation / sync limits (in ticks) ---------------------------------
initLimit=10
syncLimit=5

# --- Where the in-memory database snapshots + transaction logs live -----------
dataDir={_rel(data_dir)}

# --- The port clients connect to ---------------------------------------------
clientPort=2181
maxClientCnxns=60
autopurge.snapRetainCount=3
autopurge.purgeInterval=24

# --- The ensemble members: server.<id>=<host>:<quorum-port>:<leader-port> -----
# Each node also needs a `myid` file under dataDir containing its server id.
server.1=zk-node-1:2888:3888
server.2=zk-node-2:2888:3888
server.3=zk-node-3:2888:3888
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def _write_real_cluster_commands(path: str, stats: dict) -> None:
    """Write the authentic ZooKeeper cluster runbook (real_cluster_commands.md)."""
    md = f"""# Apache ZooKeeper — Real Production Cluster Commands

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
- Znodes created     : {stats.get('znodes_created', 0)}
- Ephemeral znodes   : {stats.get('ephemeral_nodes', 0)}
- Sessions            : {stats.get('sessions', 0)}
- Leader elected      : {stats.get('leader_elected', '<none>')}
- Watches fired       : {stats.get('watches_fired', 0)}
- Config version      : {stats.get('config_version', 0)}
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(md)


def run_coordination_demo(out_dir="data/hadoop_ecosystem/zookeeper") -> dict:
    """
    Demonstrate the three coordination patterns a news pipeline relies on.

    Returns the mandated contract dict with keys:
        znodes_created, ephemeral_nodes, sessions, leader_elected,
        watches_fired, config_version, out_dir
    """
    print("[ZooKeeper] Starting coordination-services demonstration ...")
    out_dir = _resolve(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # Fresh state each run for deterministic, reproducible results.
    zk = ZooKeeperSim(root_dir=out_dir, session_id=1000)
    zk.nodes = {"/": zk.nodes["/"]}      # keep only the root
    zk.sequence = {}
    zk.zxid = 0
    for f in (zk.tx_log_path,):
        if os.path.exists(f):
            os.remove(f)

    # Distinct simulated worker sessions taking part in coordination.
    workers = [("worker-1", 1001), ("worker-2", 1002), ("worker-3", 1003)]
    sessions_used = {zk.session_id} | {sid for _, sid in workers}

    # --- Bootstrap the coordination namespace ------------------------------
    zk.create("/news-pipeline", "", "persistent")
    zk.create("/news-pipeline/election", "", "persistent")
    zk.create("/news-pipeline/locks", "", "persistent")
    zk.create("/news-pipeline/locks/topic-model", "", "persistent")

    # Initial distributed configuration (mirrors the PySpark pipeline parameters).
    initial_config = {
        "lda_k": 6,
        "min_word_count": 10,
        "batch_size": 128,
        "spark_partitions": 8,
    }
    zk.create("/news-pipeline/config", json.dumps(initial_config), "persistent")

    # ------------------------------------------------------------------ #
    # PATTERN 1 — Leader election with ephemeral sequential znodes
    # ------------------------------------------------------------------ #
    candidates = []
    for name, sid in workers:
        # Each worker creates an ephemeral+sequential node; lowest sequence wins.
        path = zk.create("/news-pipeline/election/worker-",
                         data=f"session={sid}",
                         mode="ephemeral_sequential", owner=sid)
        candidates.append((path, sid))
    candidates.sort(key=lambda item: item[0])   # lexical order == sequence order
    leader_path, leader_sid = candidates[0]
    print(f"[ZooKeeper] Leader election: {leader_path} (session {leader_sid}) is the leader "
          f"of {len(candidates)} candidates")

    # ------------------------------------------------------------------ #
    # PATTERN 2 — Distributed configuration + watch notification
    # ------------------------------------------------------------------ #
    observed_events = []

    def make_callback(sid):
        """Return a watch callback bound to a specific worker session."""
        def _cb(path, event):
            observed_events.append({"session": sid, "path": path, "event": event})
        return _cb

    # Every worker registers a data watch on the shared config znode.
    for name, sid in workers:
        zk.add_watch("/news-pipeline/config", make_callback(sid), watch_type="data")

    # The admin bumps the configuration -> all watching workers are notified.
    updated_config = {
        "lda_k": 8,
        "min_word_count": 5,
        "batch_size": 256,
        "spark_partitions": 16,
    }
    new_stat = zk.set("/news-pipeline/config", json.dumps(updated_config))
    config_version = new_stat["version"]
    print(f"[ZooKeeper] Config changed -> version {config_version}; "
          f"{len(observed_events)} worker watch(es) notified")

    # ------------------------------------------------------------------ #
    # PATTERN 3 — Distributed lock (mutual exclusion)
    # ------------------------------------------------------------------ #
    lock_base = "/news-pipeline/locks/topic-model"
    # worker-1 grabs the lock first; worker-2 and worker-3 queue behind it.
    lock_holders = []
    for name, sid in workers:
        lock_path = zk.create(lock_base + "/lock-", data=f"owner={name}",
                              mode="ephemeral_sequential", owner=sid)
        lock_holders.append((lock_path, name, sid))
    lock_holders.sort(key=lambda item: item[0])
    first_holder_path, first_holder, _ = lock_holders[0]
    print(f"[ZooKeeper] Lock: {first_holder} holds {first_holder_path} "
          f"({len(lock_holders)} contenders — mutual exclusion enforced)")

    # The first holder finishes its critical section and its session closes,
    # releasing the lock so the next waiter is promoted.
    released = zk.close_session(lock_holders[0][2])
    remaining = sorted(zk.get_children(lock_base))
    print(f"[ZooKeeper] Lock released: {released} ephemeral node(s) cleaned up; "
          f"next holder sequence = {remaining[0] if remaining else '<none>'}")

    # ------------------------------------------------------------------ #
    # Emit the cluster artifacts + snapshots
    # ------------------------------------------------------------------ #
    _write_zoo_cfg(os.path.join(out_dir, "zoo.cfg"), os.path.join(out_dir, "data"))

    # Full znode tree snapshot (machine + human readable).
    tree = zk.build_tree("/")
    with open(os.path.join(out_dir, "znode_tree.json"), "w", encoding="utf-8") as fh:
        json.dump({
            "generated": datetime.datetime.now().isoformat(timespec="seconds"),
            "zxid": zk.zxid,
            "znode_count": len(zk.nodes),
            "tree": tree,
        }, fh, indent=2, default=str)
    with open(os.path.join(out_dir, "znode_tree.txt"), "w", encoding="utf-8") as fh:
        fh.write("ZooKeeper znode tree snapshot\n")
        fh.write("============================\n\n")
        fh.write(zk.render_text_tree("/"))
        fh.write("\n")

    # Coordination events observed during the demo (evidence of watches firing).
    with open(os.path.join(out_dir, "coordination_events.json"), "w", encoding="utf-8") as fh:
        json.dump({"leader": leader_path, "events": observed_events}, fh, indent=2, default=str)

    # Final summary dict (mandated contract).
    summary = {
        "znodes_created": zk.created_count,
        "ephemeral_nodes": zk.ephemeral_created,
        "sessions": len(sessions_used),
        "leader_elected": leader_path,
        "watches_fired": zk.watches_fired,
        "config_version": config_version,
        "out_dir": _rel(out_dir),
    }
    with open(os.path.join(out_dir, "zookeeper_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    _write_real_cluster_commands(os.path.join(out_dir, "real_cluster_commands.md"), summary)

    print(f"[ZooKeeper] Done: {summary['znodes_created']} znodes created, "
          f"{summary['ephemeral_nodes']} ephemeral, leader={leader_path}.")
    return summary


if __name__ == "__main__":
    # Run the component stand-alone and print a completion summary.
    result = run_coordination_demo()
    print("[ZooKeeper] Completion summary:")
    print(json.dumps(result, indent=2, default=str))
