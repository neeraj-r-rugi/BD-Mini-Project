# Distributed Search Engine — Implementation Guide

A mini-Google built on **HDFS + Spark + Apache Iceberg**, ranked with **BM25 + PageRank**, served by **FastAPI**.
Developed first on **one machine**, then moved to the **4-VM (ZeroTier)** deployment with *no code changes* — only a few lines in `.config` and `/etc/hosts`.

> Scope rule: implement exactly what the spec asks. Anything marked *(optional)* can be skipped.

---

## 1. Design Principles

1. **One config file.** Every non-tool setting (hosts, ports, paths, BM25 params, table names, …) lives in `./.config`. No hard-coded values in scripts.
2. **Hostnames are aliases.** Scripts and tool configs only ever say `node1 … node4`. Locally all four aliases point to `127.0.0.1`; on the VMs they point to ZeroTier IPs (set in `/etc/hosts`). This is what makes the local → 4VM switch trivial.
3. **Folders = VMs.** Each top-level folder holds exactly the scripts that run on that VM.
4. **Rebuild, don't patch.** After each phase the index and PageRank are recomputed from a *pinned Iceberg snapshot*. Simple, deterministic, idempotent.
5. **Publish atomically.** Exports are written to a temp location, validated, then renamed. Node 4 only switches versions after validation passes.

---

## 2. Repository Layout

```
.
├── .config                      # ALL project config (single source of truth)
├── config_loader.py             # reads .config, builds SparkSession (only root .py file)
│
├── Ingestion_Node/              # Node 1  – Spark *client*, no daemons
│   ├── data/                    #   pages.jsonl, changes.jsonl (local disk)
│   ├── upload_to_hdfs.sh        #   put raw phase file into HDFS
│   ├── ingest.py                #   validate → dedupe → MERGE into Iceberg
│   ├── run_ingestion.sh         #   <phase>: upload + ingest
│   └── reports/                 #   per-phase ingestion report (json)
│
├── Cluster_1/                   # Node 2  – HDFS NameNode + DataNode + Spark worker
│   ├── start_node.sh / stop_node.sh
│   ├── init_tables.py           #   create Iceberg namespace + table (once)
│   └── iceberg_admin.py         #   snapshots | checksum | rollback | timetravel
│
├── Cluster_2/                   # Node 3  – Spark master + worker + HDFS DataNode
│   ├── start_node.sh / stop_node.sh
│   ├── build_index.py           #   inverted index
│   ├── pagerank.py              #   PageRank (≥10 iterations)
│   ├── export.py                #   validate + write plain Parquet
│   ├── run_pipeline.sh          #   <version>: index → pagerank → export → stage
│   └── export_staging/          #   local copy of export (Node 4 pulls from here)
│
└── Search_Node/                 # Node 4  – FastAPI + UI, no Spark/HDFS
    ├── pull_export.sh           #   <version>: fetch, validate, hot-reload
    ├── search_service.py        #   FastAPI app (BM25 + PageRank)
    ├── static/index.html        #   basic frontend
    └── serving/                 #   v0/, v1/, current -> v1
```

| Node | Folder | Runs |
|---|---|---|
| 1 | `Ingestion_Node` | Spark driver for ingestion (client only) |
| 2 | `Cluster_1` | HDFS NameNode, DataNode, Spark worker; Iceberg maintenance |
| 3 | `Cluster_2` | Spark master, Spark worker, HDFS DataNode; drivers for index / PageRank / export |
| 4 | `Search_Node` | FastAPI search service + UI |

---

## 3. Configuration

### 3.1 `.config` (plain `KEY=VALUE`, bash-sourceable)

Rules: no inline comments (comments on their own lines), no spaces around `=`, `${VAR}` interpolation allowed only for variables defined *above*.

```bash
# ===== ENVIRONMENT-SPECIFIC: the ONLY lines edited when moving local -> 4 VMs =====
# 1 on a single machine, 2 on the 4-VM cluster
HDFS_REPLICATION=1
# false locally (Node2 already runs the only DataNode), true on VMs
CLUSTER2_RUN_DATANODE=false
# user for rsync/ssh from Node 4 to Node 3 (ignored locally)
SSH_USER=youruser

# ===== TOPOLOGY (aliases resolved via /etc/hosts) =====
NODE1_HOST=node1
NODE2_HOST=node2
NODE3_HOST=node3
NODE4_HOST=node4

HDFS_PORT=9000
HDFS_URI=hdfs://${NODE2_HOST}:${HDFS_PORT}
SPARK_MASTER_PORT=7077
SPARK_MASTER_URL=spark://${NODE3_HOST}:${SPARK_MASTER_PORT}

# ===== SPARK (client-mode drivers need fixed ports so executors can connect back) =====
SPARK_DRIVER_PORT=40000
SPARK_BLOCKMANAGER_PORT=40001
SPARK_EXECUTOR_MEMORY=1g
SPARK_CORES_MAX=4
SPARK_SHUFFLE_PARTITIONS=8
# must match your installed Spark/Scala/Iceberg versions
ICEBERG_PACKAGE=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2

# ===== HDFS LAYOUT =====
HDFS_BASE=/search_engine
HDFS_RAW_DIR=${HDFS_BASE}/raw
HDFS_WAREHOUSE=${HDFS_BASE}/warehouse
HDFS_INDEX_DIR=${HDFS_BASE}/index
HDFS_EXPORT_DIR=${HDFS_BASE}/export
HDFS_REJECT_DIR=${HDFS_BASE}/rejected
HDFS_CHECKPOINT_DIR=${HDFS_BASE}/checkpoints

# ===== ICEBERG =====
ICEBERG_CATALOG=lake
ICEBERG_NAMESPACE=search
ICEBERG_DOCS_TABLE=docs

# ===== DATA / INGESTION =====
LOCAL_DATA_DIR=Ingestion_Node/data
PHASE_0_FILE=pages.jsonl
PHASE_1_FILE=changes.jsonl
# optional: a deliberately bad change file for the rollback demo
PHASE_2_FILE=bad_changes.jsonl
REPORT_DIR=Ingestion_Node/reports
REQUIRED_FIELDS=doc_id,url,title,text

# ===== INDEX / RANKING =====
# tokenizer used by BOTH the index job and the search service
TOKEN_REGEX=[a-z0-9]+
BM25_K1=1.2
BM25_B=0.75
PAGERANK_DAMPING=0.85
PAGERANK_ITERATIONS=10
W_BM25=0.7
W_PAGERANK=0.3

# ===== EXPORT / SERVING =====
STAGING_DIR=Cluster_2/export_staging
SERVING_DIR=Search_Node/serving
SEARCH_PORT=8000
TOP_K=10
```

### 3.2 What changes between local and 4-VM

| Item | Local (1 machine) | 4 VMs |
|---|---|---|
| `/etc/hosts` | `127.0.0.1 node1 node2 node3 node4` | `<zt-ip-1> node1` … `<zt-ip-4> node4` (on **every** VM) |
| `.config` → `HDFS_REPLICATION` | `1` | `2` |
| `.config` → `CLUSTER2_RUN_DATANODE` | `false` | `true` |
| `.config` → `SSH_USER` | – | real user |
| Code / tool XML / scripts | **identical** | **identical** |

### 3.3 `config_loader.py` (repo root)

Small shared module — parses `.config` the same way bash does and builds Spark sessions.

```python
import re
from pathlib import Path
ROOT = Path(__file__).resolve().parent

def _load():
    cfg = {}
    for line in (ROOT / ".config").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = re.sub(r"\$\{(\w+)\}", lambda m: cfg.get(m.group(1), ""), v.strip().strip("'\""))
        cfg[k.strip()] = v
    return cfg

CFG = _load()

def path(key):                      # relative paths resolve against repo root
    p = Path(CFG[key]);  return p if p.is_absolute() else ROOT / p

def table(name=None):               # e.g. lake.search.docs
    return f"{CFG['ICEBERG_CATALOG']}.{CFG['ICEBERG_NAMESPACE']}.{name or CFG['ICEBERG_DOCS_TABLE']}"

def hdfs(key):                      # full hdfs:// URI for a *_DIR key
    return CFG["HDFS_URI"] + CFG[key]

def build_spark(app, node):         # node = 1..4 : which machine hosts this driver
    from pyspark.sql import SparkSession
    c, cat = CFG, CFG["ICEBERG_CATALOG"]
    spark = (SparkSession.builder.appName(app)
        .master(c["SPARK_MASTER_URL"])
        .config("spark.driver.host", c[f"NODE{node}_HOST"])
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", c["SPARK_DRIVER_PORT"])
        .config("spark.blockManager.port", c["SPARK_BLOCKMANAGER_PORT"])
        .config("spark.jars.packages", c["ICEBERG_PACKAGE"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "hadoop")
        .config(f"spark.sql.catalog.{cat}.warehouse", hdfs("HDFS_WAREHOUSE"))
        .config("spark.hadoop.dfs.replication", c["HDFS_REPLICATION"])
        .config("spark.sql.shuffle.partitions", c["SPARK_SHUFFLE_PARTITIONS"])
        .config("spark.executor.memory", c["SPARK_EXECUTOR_MEMORY"])
        .config("spark.cores.max", c["SPARK_CORES_MAX"])
        .getOrCreate())
    spark.sparkContext.setCheckpointDir(hdfs("HDFS_CHECKPOINT_DIR"))
    return spark
```

Every Python script starts with:

```python
import sys; from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import CFG, build_spark, table, hdfs, path
```

Every shell script starts with:

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }
```

> **Run Python scripts with plain `python script.py`** — `build_spark()` sets the master and connects in client mode, so `spark-submit` is not needed.

---

## 4. One-Time Setup Notes (tool-specific, not in `.config`)

Do these once per machine. Because only aliases appear in these files, they are the same locally and on VMs.

**Environment variables (all nodes):** `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, `HADOOP_CONF_DIR`, and `PYSPARK_PYTHON` pointing to the *same Python minor version* on Nodes 1–3 (mismatch = cryptic worker crashes).

**`/etc/hosts` (all nodes):** define `node1..node4` as in §3.2.

**Hadoop (`core-site.xml`, `hdfs-site.xml`) on Nodes 2 & 3:**

| Property | Value | Why |
|---|---|---|
| `fs.defaultFS` | `hdfs://node2:9000` | NameNode address |
| `dfs.namenode.rpc-bind-host` | `0.0.0.0` | otherwise NN listens only on localhost and Node 3 can't connect |
| `dfs.datanode.use.datanode.hostname` / `dfs.client.use.datanode.hostname` | `true` | clients reach DataNodes by alias, not by an unreachable internal IP |
| `dfs.namenode.datanode.registration.ip-hostname-check` | `false` | avoids DataNode registration rejections |
| `dfs.namenode.name.dir`, `dfs.datanode.data.dir` | local dirs | storage locations |
| `dfs.replication` | leave default | the real value is set per write from `HDFS_REPLICATION` |

Format the NameNode **once** on Node 2: `hdfs namenode -format`.
Node 3's DataNode needs the same `core-site.xml` (pointing at `node2`). Nodes 1 and 4 need **no** Hadoop config — scripts use full `hdfs://…` URIs. (Node 1 only needs the Hadoop *client binaries* for `hdfs dfs -put`.)

**Spark (`spark-env.sh`):**
- Node 3: `SPARK_MASTER_HOST=node3`
- Nodes 2 & 3: `SPARK_LOCAL_IP=<this node's alias or ZeroTier IP>`, and optionally `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`.
- Node 1 (driver for ingestion) & Node 3: driver host/ports come from `.config` via `build_spark()`; executors must be able to reach `SPARK_DRIVER_PORT` and `SPARK_BLOCKMANAGER_PORT` on the driver machine.

**Iceberg:** the runtime jar is pulled by `spark.jars.packages` (needs internet on first run; offline → download the jar and put it in `$SPARK_HOME/jars` on Nodes 1–3 and remove the `.config("spark.jars.packages")` line's dependency by leaving `ICEBERG_PACKAGE` unused). Iceberg and Spark/Scala versions must match. A **Hadoop catalog** on HDFS is used (no Hive metastore needed).

**Python packages:**
- Nodes 1–3: `pyspark` (same version as the cluster Spark).
- Node 4: `fastapi uvicorn pandas pyarrow numpy`.

**ZeroTier / firewall — ports that must be reachable between VMs:**
`9000` (NN RPC), `9870` (NN UI), `9864/9866/9867` (DN), `7077` (Spark master), `8080/8081` (Spark UIs), `4040` (driver UI), `40000/40001` (driver + block manager), `8000` (search), `22` (ssh/rsync). ZeroTier's default is open, but OS firewalls (ufw) may block.

**SSH:** passwordless key from **Node 4 → Node 3** (for `rsync`).

**Clone the repo to the same absolute path on every VM** (e.g. `~/search-engine`) — the rsync step relies on this.

---

## 5. Data Contract (verify against the real files first!)

Phase 0 `pages.jsonl`: one JSON object per line.

```json
{"doc_id": "...", "url": "...", "title": "...", "text": "...", "links": ["id1","id2"], "version": 1}
```

Phase 1 `changes.jsonl`:

```json
{"doc_id": "phase1-new-025", "url": "...", "title": "...", "text": "...", "links": ["333"], "op": "UPSERT"}
{"doc_id": "14", "op": "DELETE"}
```

**Assumptions** (confirm in Step 0; adjust if the data differs):
- `op` missing ⇒ `UPSERT`.
- `UPSERT` requires non-blank `doc_id, url, title, text` (`REQUIRED_FIELDS`); `DELETE` requires only `doc_id`.
- `links` missing/null ⇒ empty list. Link values are doc IDs (cast to string).
- `version` may be absent; "highest version wins" is applied **within an input batch**; ties (and missing versions) are broken by *later line wins*.
- Change-set records are authoritative: an `UPSERT` always overwrites the table row, a `DELETE` always removes it.

**Iceberg table** `lake.search.docs`:

| column | type |
|---|---|
| doc_id | string |
| url | string |
| title | string |
| text | string |
| links | array&lt;string&gt; |
| version | bigint |

---

## 6. Step-by-Step Guide

### Step 0 — Profile the data (10 min)

```bash
cd Ingestion_Node/data
wc -l pages.jsonl changes.jsonl
# find malformed JSON lines
awk '{print NR": "$0}' pages.jsonl | while IFS= read -r l; do echo "${l#*: }" | jq -e . >/dev/null 2>&1 || echo "bad line ${l%%:*}"; done
# duplicates by doc_id
jq -r .doc_id pages.jsonl 2>/dev/null | sort | uniq -d | head
# key sets actually present
jq -c 'keys' pages.jsonl 2>/dev/null | sort | uniq -c
```

Note the counts of malformed / duplicate / missing-field / dangling-link records — you will compare them against the ingestion report in Step 3. Also record the 4 deleted IDs (e.g. `14, 17, 18, 19`) and their titles for the Phase 1 demo.

### Step 1 — Start the clusters

Start order matters: **NameNode → Spark master → workers/DataNodes.**

`Cluster_1/start_node.sh` (Node 2):
1. Create dirs if missing; if the NameNode isn't formatted, abort with a hint.
2. `hdfs --daemon start namenode` ; `hdfs --daemon start datanode`
3. Start the Spark worker: `$SPARK_HOME/sbin/start-worker.sh "$SPARK_MASTER_URL"` (set `SPARK_WORKER_DIR=$(abs Cluster_1/work)`).

`Cluster_2/start_node.sh` (Node 3):
1. `$SPARK_HOME/sbin/start-master.sh`
2. `$SPARK_HOME/sbin/start-worker.sh "$SPARK_MASTER_URL"` (`SPARK_WORKER_DIR=$(abs Cluster_2/work)`)
3. `if [ "$CLUSTER2_RUN_DATANODE" = true ]; then hdfs --daemon start datanode; fi`

Matching `stop_node.sh` scripts reverse these. **Locally**, run both scripts on your one machine (Node 2 first); the worker web UI auto-increments to 8082 for the second worker, giving you two Spark workers like the real cluster.

**Check:**
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity"   # 1 local / 2 on VMs
# Spark UI: http://node3:8080  → 2 workers ALIVE
```

Create the HDFS layout once:
```bash
for d in RAW_DIR WAREHOUSE INDEX_DIR EXPORT_DIR REJECT_DIR CHECKPOINT_DIR; do
  v=HDFS_$d; hdfs dfs -mkdir -p "$HDFS_URI${!v}"; done
```
(put this in `Cluster_1/start_node.sh`, guarded by `-mkdir -p`).

### Step 2 — Create the Iceberg table (Node 2)

`Cluster_1/init_tables.py`:

```python
spark = build_spark("init-tables", node=2)
cat, ns = CFG["ICEBERG_CATALOG"], CFG["ICEBERG_NAMESPACE"]
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{ns}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {table()} (
    doc_id string, url string, title string, text string,
    links array<string>, version bigint) USING iceberg""")
```

`Cluster_1/iceberg_admin.py` — a tiny CLI (argparse) with these subcommands, all printing to stdout:

| Command | Does |
|---|---|
| `snapshots` | `SELECT snapshot_id, committed_at, operation, summary FROM <table>.snapshots ORDER BY committed_at` |
| `current-snapshot` | prints the latest `snapshot_id` only (used by scripts) |
| `checksum [--snapshot ID]` | `count(*)` + `sum(hash(doc_id,url,title,text,links,version))` at a snapshot — a content fingerprint |
| `timetravel ID` | row count + sample rows at that snapshot (`option("snapshot-id", ID)`) |
| `rollback ID` | `CALL lake.system.rollback_to_snapshot('search.docs', ID)` |

### Step 3 — Ingestion (Node 1)

`Ingestion_Node/upload_to_hdfs.sh <phase>`:

```bash
FILE=PHASE_${1}_FILE
hdfs dfs -D dfs.replication="$HDFS_REPLICATION" -put -f \
  "$(abs "$LOCAL_DATA_DIR")/${!FILE}" "$HDFS_URI$HDFS_RAW_DIR/${!FILE}"
```

`Ingestion_Node/run_ingestion.sh <phase>` = upload, then `python ingest.py --phase <phase>`.

`ingest.py` does, in order:

**(a) Read leniently, capturing bad JSON**
```python
schema = ("doc_id string, url string, title string, text string, links array<string>, "
          "version bigint, op string, _corrupt string")
raw = (spark.read.schema(schema).option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt").json(hdfs_input_path)).cache()
raw = (raw.withColumn("op", F.upper(F.coalesce("op", F.lit("UPSERT"))))
          .withColumn("_line", F.monotonically_increasing_id()))
```

**(b) Validate**
```python
def blank(c): return F.col(c).isNull() | (F.trim(F.col(c)) == "")
bad_json = raw.filter("_corrupt IS NOT NULL")
rest     = raw.filter("_corrupt IS NULL")
invalid  = blank("doc_id") | ~F.col("op").isin("UPSERT", "DELETE") | \
           ((F.col("op") == "UPSERT") & (blank("url") | blank("title") | blank("text")))
bad_fields, ok = rest.filter(invalid), rest.filter(~invalid)
```
Write `bad_json ∪ bad_fields` to `HDFS_REJECT_DIR/phase_<n>` (so rejected rows are inspectable).

**(c) Deduplicate by `doc_id` — highest version wins**
```python
w = W.partitionBy("doc_id").orderBy(F.col("version").desc_nulls_last(), F.col("_line").desc())
batch = ok.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
staged = batch.select("doc_id", "url", "title", "text",
                      F.array_distinct(F.coalesce("links", F.array().cast("array<string>"))).alias("links"),
                      "version", "op")
staged.createOrReplaceTempView("staged")
```

**(d) Remember the snapshot before merging** (`prev = latest snapshot_id or None`).

**(e) MERGE into Iceberg**
```sql
MERGE INTO lake.search.docs t USING staged s ON t.doc_id = s.doc_id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.op = 'UPSERT' THEN UPDATE SET
     t.url = s.url, t.title = s.title, t.text = s.text, t.links = s.links, t.version = s.version
WHEN NOT MATCHED AND s.op = 'UPSERT' THEN INSERT
     (doc_id, url, title, text, links, version) VALUES (s.doc_id, s.url, s.title, s.text, s.links, s.version)
```
Re-running the same phase produces identical table *contents* (upserts rewrite identical rows, deletes find nothing).

**(f) Remove dangling links** — run over the *whole table* (deleted docs also orphan links from other docs):
```python
docs = spark.table(T)
ids  = docs.select(F.col("doc_id").alias("valid_id"))
kept = (docs.select("doc_id", F.explode("links").alias("l"))
            .join(ids, F.col("l") == F.col("valid_id"), "left_semi")
            .groupBy("doc_id").agg(F.collect_list("l").alias("new_links")))
fixed = (docs.select("doc_id", "links").join(kept, "doc_id", "left")
         .withColumn("new_links", F.coalesce("new_links", F.array().cast("array<string>")))
         .filter(F.size("links") != F.size("new_links")))
if fixed.take(1):
    fixed.createOrReplaceTempView("fixed")
    spark.sql(f"MERGE INTO {T} t USING fixed s ON t.doc_id = s.doc_id "
              "WHEN MATCHED THEN UPDATE SET t.links = s.new_links")
```
(Only rows that actually change are touched, so a repeated run makes no extra changes.)

**(g) Detect changes via Iceberg snapshots** — read the table at `prev` and at the new current snapshot (`option("snapshot-id", id)`), then compute with `doc_id`/row diffs: `added`, `deleted`, `updated` counts. Dump to `Ingestion_Node/reports/phase_<n>.json` together with: rows read, malformed, missing-fields, duplicates dropped, accepted, dangling links removed, old/new snapshot IDs.

**Run & verify (Phase 0):**
```bash
python Cluster_1/init_tables.py          # once
Ingestion_Node/run_ingestion.sh 0
python Cluster_1/iceberg_admin.py snapshots
python Cluster_1/iceberg_admin.py checksum      # save this value as CHK0
```
Compare `reports/phase_0.json` with your Step 0 numbers — they must agree.

### Step 4 — Distributed inverted index (Node 3)

`Cluster_2/build_index.py --snapshot <id>`:

```python
docs = spark.read.option("snapshot-id", snap).format("iceberg").load(table())
tok  = F.expr(f"regexp_extract_all(lower(concat_ws(' ', title, text)), '{CFG['TOKEN_REGEX']}', 0)")
toks = docs.select("doc_id", tok.alias("tokens"))
doc_stats = toks.select("doc_id", F.size("tokens").alias("doc_len"))
postings  = (toks.select("doc_id", F.explode("tokens").alias("term"))
                 .groupBy("term", "doc_id").agg(F.count("*").alias("tf")))
postings.write.mode("overwrite").parquet(hdfs("HDFS_INDEX_DIR") + "/postings")
doc_stats.write.mode("overwrite").parquet(hdfs("HDFS_INDEX_DIR") + "/doc_stats")
```

- Title + text are indexed; `doc_len` counts those same tokens (consistent with BM25's `len(D)`).
- The `explode → groupBy` shuffles across both workers — that is your "distributed" index. Screenshot the stage view at `http://node3:4040` for the report.
- `df(t)` is derived later as the number of posting rows per term; `N` and `avgdl` from `doc_stats`/`docs`.
- "Update" = rebuild from the new snapshot (full overwrite). Simple and always consistent.

### Step 5 — PageRank (Node 3)

`Cluster_2/pagerank.py --snapshot <id>` — damping `0.85`, `PAGERANK_ITERATIONS` (≥10), dangling-node mass redistributed uniformly:

```python
d, ITER = float(CFG["PAGERANK_DAMPING"]), int(CFG["PAGERANK_ITERATIONS"])
docs  = spark.read.option("snapshot-id", snap).format("iceberg").load(table()).select("doc_id", "links")
N     = docs.count()
nodes = docs.select(F.col("doc_id").alias("id"))
edges = docs.select(F.col("doc_id").alias("src"), F.explode("links").alias("dst"))
outdeg = edges.groupBy("src").agg(F.count("*").alias("deg"))
links  = edges.join(outdeg, "src").cache()                          # src, dst, deg
sinks  = nodes.join(outdeg, nodes.id == outdeg.src, "left_anti")   # no out-links
ranks  = nodes.withColumn("rank", F.lit(1.0 / N))

for _ in range(ITER):
    contribs = (links.join(ranks, links.src == ranks.id)
                .select(F.col("dst").alias("id"), (F.col("rank") / F.col("deg")).alias("c"))
                .groupBy("id").agg(F.sum("c").alias("s")))
    sink_mass = ranks.join(sinks, "id").agg(F.sum("rank")).first()[0] or 0.0
    ranks = (nodes.join(contribs, "id", "left")
             .select("id", (F.lit((1 - d) / N) +
                      F.lit(d) * (F.coalesce("s", F.lit(0.0)) + F.lit(sink_mass / N))).alias("rank"))
             .checkpoint())          # reliable checkpoint on HDFS: short lineage + fault tolerance

ranks.select(F.col("id").alias("doc_id"), F.col("rank").alias("pagerank")) \
     .write.mode("overwrite").parquet(hdfs("HDFS_INDEX_DIR") + "/pagerank")
```

Sanity check: `sum(rank) ≈ 1.0` after the last iteration (also re-checked in Step 6).

### Step 6 — Validate & export (Node 3)

`Cluster_2/export.py --snapshot <id> --version <n>` reads the index/PageRank Parquet plus `url`/`title` from the same snapshot, then **validates**:

- `docs` count > 0 and equals the pagerank row count
- `|sum(pagerank) − 1| < 1e-6`
- no posting references a `doc_id` not in `docs`; no null `url`

Any failure → `raise` (non-zero exit, nothing published). On success it writes **plain Parquet** to `HDFS_EXPORT_DIR/v<n>.tmp/`:

| dataset | columns | notes |
|---|---|---|
| `postings` | term, doc_id, tf | `coalesce(1)` |
| `docs` | doc_id, url, title, doc_len, pagerank | `coalesce(1)` |
| `manifest` | version, snapshot_id, n_docs, n_postings, created_at | 1 row |

`Cluster_2/run_pipeline.sh <version>` (version = phase number):

```bash
SNAP=$(python "$ROOT/Cluster_1/iceberg_admin.py" current-snapshot)      # pin ONE snapshot for all jobs
python "$ROOT/Cluster_2/build_index.py" --snapshot "$SNAP"
python "$ROOT/Cluster_2/pagerank.py"    --snapshot "$SNAP"
python "$ROOT/Cluster_2/export.py"      --snapshot "$SNAP" --version "$1"   # fails => script stops (set -e)
E="$HDFS_URI$HDFS_EXPORT_DIR"
hdfs dfs -rm -r -f "$E/v$1"; hdfs dfs -mv "$E/v$1.tmp" "$E/v$1"            # atomic publish in HDFS
S="$(abs "$STAGING_DIR")"; rm -rf "$S/v$1"; mkdir -p "$S"
hdfs dfs -get "$E/v$1" "$S/v$1"                                            # stage locally on Node 3
```

### Step 7 — Search service & UI (Node 4)

**`Search_Node/pull_export.sh <version>`**

1. If `NODE3_HOST` resolves to this machine (local mode): `cp -r` from `STAGING_DIR`; otherwise `rsync -az --delete $SSH_USER@$NODE3_HOST:$ROOT/Cluster_2/export_staging/v<n>/ $SERVING_DIR/v<n>.tmp/`. (Detect local with `getent hosts node3` vs `127.0.0.1`.)
2. Check `_SUCCESS` exists inside `postings/`, `docs/`, `manifest/`; else delete the `.tmp` and exit 1.
3. `mv v<n>.tmp v<n>`.
4. `curl -s -o /dev/null -w "%{http_code}" -X POST "localhost:$SEARCH_PORT/reload?version=<n>"`
   - `200` → `ln -sfn v<n> $SERVING_DIR/current` (switch committed).
   - other HTTP code → service rejected it: `mv v<n> v<n>.rejected`, keep serving the old version, exit 1.
   - `000` (service not running yet) → just update `current` and print "start the service".

**`search_service.py`** (FastAPI; run with `uvicorn search_service:app --host 0.0.0.0 --port $SEARCH_PORT`)

State = one immutable object swapped atomically on reload:

```python
def load_version(v):
    d = SERVING / f"v{v}"
    for n in ("postings", "docs", "manifest"):
        if not (d / n / "_SUCCESS").exists(): raise ValueError(f"{n} incomplete")
    docs, post, man = (pd.read_parquet(d / n) for n in ("docs", "postings", "manifest"))
    if len(docs) != man.n_docs[0] or len(post) != man.n_postings[0]: raise ValueError("count mismatch")
    idx = {d_: i for i, d_ in enumerate(docs.doc_id)}
    post["i"] = post.doc_id.map(idx)
    inv = {t: (g.i.to_numpy(), g.tf.to_numpy(float)) for t, g in post.groupby("term")}
    return dict(version=v, N=len(docs), avgdl=docs.doc_len.mean(), doc_len=docs.doc_len.to_numpy(float),
                pr=docs.pagerank.to_numpy(), max_pr=docs.pagerank.max(),
                ids=docs.doc_id.tolist(), urls=docs.url.tolist(), titles=docs.title.tolist(), inv=inv)
```

Scoring for `GET /search?q=...&k=10`:

```python
def search(q, k):
    s, scores = STATE, {}
    for t in set(re.findall(TOKEN_REGEX, q.lower())):          # same tokenizer as the index job
        if t not in s["inv"]: continue
        idx, tf = s["inv"][t]; df = len(idx)
        idf  = math.log(1 + (s["N"] - df + 0.5) / (df + 0.5))
        part = idf * tf * (K1 + 1) / (tf + K1 * (1 - B + B * s["doc_len"][idx] / s["avgdl"]))
        for i, p in zip(idx, part): scores[i] = scores.get(i, 0.0) + p
    if not scores: return []
    mb = max(scores.values())
    ranked = sorted(((W_BM25 * b / mb + W_PR * s["pr"][i] / s["max_pr"], i, b) for i, b in scores.items()),
                    reverse=True)[:k]
    return [dict(rank=r+1, doc_id=s["ids"][i], url=s["urls"][i], title=s["titles"][i],
                 bm25=b, pagerank=float(s["pr"][i]), score=sc) for r, (sc, i, b) in enumerate(ranked)]
```

Choices to state in your report: terms are OR-combined; the IDF form above is the common Lucene variant — **use whatever IDF the course's "code snippet" uses if it differs**; BM25 is normalised by the max over *matching* documents for the query, PageRank by the *global* max.

Other endpoints:

| Endpoint | Behaviour |
|---|---|
| `GET /search?q=&k=` | ranked JSON (doc_id, url, title, bm25, pagerank, score) + current `version` |
| `GET /version` | loaded version + manifest info |
| `POST /reload?version=N` | `load_version(N)` in a try-block; on success swap `STATE`; on any error return HTTP 400 and **leave `STATE` untouched** |
| `GET /` | serves `static/index.html` |

On startup, load the version `SERVING_DIR/current` points to (if none, `/search` returns 503).

**`static/index.html`** — one page: input box, Search button, a results table (rank, title, doc_id, URL link, score), and a header showing the loaded version from `/version`. Plain `fetch('/search?q=' + encodeURIComponent(q))`; no framework needed.

### Step 8 — Run Phase 0 end to end, then Phase 1

```bash
# --- Phase 0 ---
Ingestion_Node/run_ingestion.sh 0
Cluster_2/run_pipeline.sh 0
Search_Node/pull_export.sh 0
uvicorn search_service:app --app-dir Search_Node --host 0.0.0.0 --port 8000     # first time only
# query: http://node4:8000  — save screenshots/JSON of 3–4 queries

# --- Phase 1 (after observing Phase 0) ---
Ingestion_Node/run_ingestion.sh 1
Cluster_2/run_pipeline.sh 1
Search_Node/pull_export.sh 1        # hot-reloads; v0 stays on disk
```

**Phase 1 demo checklist**
- Search a term unique to a deleted doc (14/17/18/19) → present in v0, **absent** in v1.
- Search `phase` / `added during project phase 1` → the `phase1-new-*` docs appear only in v1.
- Same query, before vs after: show ranking changes (PageRank shifts because links to/from deleted/new docs changed).
- `GET /reload?version=0` rolls the service back to the Phase 0 corpus instantly.

### Step 9 — Required demonstrations

**9.1 Idempotent ingestion**
```bash
python Cluster_1/iceberg_admin.py checksum          # CHK1
Ingestion_Node/run_ingestion.sh 1                   # ingest same phase again
python Cluster_1/iceberg_admin.py checksum          # must equal CHK1
```

**9.2 Time travel & recovery from a bad update**
```bash
python Cluster_1/iceberg_admin.py snapshots                    # note phase-0 and phase-1 snapshot IDs
python Cluster_1/iceberg_admin.py timetravel <phase0_snap>     # old data still readable
# make Ingestion_Node/data/bad_changes.jsonl (e.g. DELETE ~half the doc_ids), then:
Ingestion_Node/run_ingestion.sh 2                              # "bad update"
python Cluster_1/iceberg_admin.py rollback <phase1_snap>
python Cluster_1/iceberg_admin.py checksum                     # equals CHK1 again
```

**9.3 Spark fault tolerance** (the 10–15 min pipeline gives you plenty of time)
1. Start `Cluster_2/run_pipeline.sh 1`.
2. While the PageRank stage runs, kill one worker: `kill -9 $(jps | awk '/Worker/{print $1}')` on Node 2 (or Node 3).
3. Watch `http://node3:8080` / `:4040`: executor lost → tasks re-queued → job **finishes successfully** on the remaining worker.
4. Restart the worker (`start_node.sh`) and show it re-registers.
5. Explain in the report: lineage recomputation + `spark.task.maxFailures` retries + HDFS-checkpointed PageRank state.

**9.4 HDFS replication** *(4-VM only; with replication 1 locally there is nothing to demonstrate)*
```bash
hdfs fsck /search_engine -files -blocks -locations   # every block shows 2 replicas
# stop the DataNode on Node 3 → files still readable, fsck reports under-replicated blocks
hdfs dfs -cat $HDFS_URI/search_engine/export/v1/docs/*.parquet | head -c 100 >/dev/null
# restart it → blocks re-replicate
```

**9.5 Failed export never goes live**
Corrupt a staged export (delete a Parquet file or the `_SUCCESS` of `postings/` inside `Cluster_2/export_staging/v1`), run `pull_export.sh 1` → script exits non-zero, service keeps serving the previous version; show `GET /version` unchanged.

### Step 10 — Switching from local to 4 VMs (checklist)

1. Provision ZeroTier; note each VM's ZeroTier IP.
2. On **every** VM: `/etc/hosts` → `node1..node4` = real IPs. Clone the repo to the same path. Install tools per §4 (Node 1: Spark client + Hadoop client; Node 4: Python deps only).
3. Edit `.config`: `HDFS_REPLICATION=2`, `CLUSTER2_RUN_DATANODE=true`, `SSH_USER=<user>`. Copy the file to all VMs (or `git pull`).
4. Set up SSH key Node 4 → Node 3.
5. Format NameNode on Node 2 only; start **Node 2 → Node 3**.
6. Verify: `hdfs dfsadmin -report` = 2 live DataNodes; Spark UI = 2 workers.
7. Run on the right VM — `init_tables.py` (Node 2) → `run_ingestion.sh` (Node 1) → `run_pipeline.sh` (Node 3) → `pull_export.sh` + `uvicorn` (Node 4).
8. Re-run Steps 8–9 on the **main corpus** (final submission must not use the toy dataset).

> During development use the **toy dataset** locally by pointing `PHASE_0_FILE`/`PHASE_1_FILE` at the toy files; switch back for the final run.

---

## 7. Cheat Sheet — Run Order

```
Node 2: Cluster_1/start_node.sh            (NN + DN + worker)
Node 3: Cluster_2/start_node.sh            (master + worker [+ DN])
Node 2: python Cluster_1/init_tables.py    (once)
Node 1: Ingestion_Node/run_ingestion.sh N
Node 3: Cluster_2/run_pipeline.sh N
Node 4: Search_Node/pull_export.sh N       (service must be running for hot reload)
```

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Job hangs at "Initial job has not accepted any resources" | Workers not registered, or `SPARK_EXECUTOR_MEMORY`/`SPARK_CORES_MAX` exceeds what workers offer |
| Executors can't connect back / `Connection refused` to driver | `NODEn_HOST` alias wrong on the worker's `/etc/hosts`; driver ports `40000/40001` blocked |
| `Connection refused …:9000` from Node 3 | NameNode bound to localhost → set `dfs.namenode.rpc-bind-host=0.0.0.0` |
| DataNode not showing in report | Wrong `fs.defaultFS` on Node 3, or clusterID mismatch after re-formatting NN (clear DN data dir) |
| `NoSuchMethodError` / class errors from Iceberg | Iceberg jar doesn't match Spark/Scala version → fix `ICEBERG_PACKAGE` |
| `Python in worker has different version` | Set the same `PYSPARK_PYTHON` on Nodes 1–3 |
| Ivy/Maven download fails | No internet → place Iceberg jar in `$SPARK_HOME/jars` on Nodes 1–3 |
| "queries from raw JSON files cannot reference only `_corrupt`" | Call `.cache()` on the raw DataFrame (as in Step 3a) |
| HDFS in safe mode | `hdfs dfsadmin -safemode leave` after all DataNodes are up |
| `MERGE` not supported | Missing `spark.sql.extensions` (set in `build_spark`) or Spark < 3.3 / Iceberg < 1.0 |
| Service shows old results after pull | `pull_export.sh` couldn't reach `/reload` (check `SEARCH_PORT`), or `current` symlink wasn't updated |

---

## 9. Deliverables Checklist (maps to spec §7)

| Spec deliverable | Where it's shown |
|---|---|
| Phase-based ingestion + lakehouse tables | Step 3, `reports/phase_*.json`, `iceberg_admin.py snapshots` |
| Validation / dirty-record handling | Step 3 (b)(c)(f), `HDFS_REJECT_DIR`, report counts |
| Distributed inverted index | Step 4 (+ Spark UI screenshot) |
| PageRank ≥ 10 iterations | Step 5 (`PAGERANK_ITERATIONS=10`) |
| Spark fault tolerance | Step 9.3 (+ 9.4 for HDFS) |
| BM25 ranking | Step 7 scoring (`k1=1.2`, `b=0.75`, final = 0.7·BM25n + 0.3·PRn) |
| Search service + frontend on Node 4 | Step 7 |
| Queries before/after changes | Step 8 Phase 1 demo checklist |
