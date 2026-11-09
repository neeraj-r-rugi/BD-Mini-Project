# Scaffolding — Distributed Search Engine

Skeletons for every script in the repo. Each file has its signature, docstrings and `TODO` comments only; fill in the bodies by following the matching step in `README.md`.

Conventions used everywhere:
- Python scripts are run with plain `python <script>.py` (no `spark-submit`).
- Every value comes from `.config` via `config_loader.CFG`. No hard-coded hosts, ports, paths or parameters.
- Python scripts print results to **stdout** and logs/progress to **stderr**, so shell scripts can capture stdout safely (e.g. `SNAP=$(python ... current-snapshot)`).

Contents

1. [Root](#1-root) — `config_loader.py`
2. [Ingestion_Node](#2-ingestion_node) — `upload_to_hdfs.sh`, `run_ingestion.sh`, `ingest.py`
3. [Cluster_1](#3-cluster_1) — `start_node.sh`, `stop_node.sh`, `init_tables.py`, `iceberg_admin.py`
4. [Cluster_2](#4-cluster_2) — `start_node.sh`, `stop_node.sh`, `build_index.py`, `pagerank.py`, `export.py`, `run_pipeline.sh`
5. [Search_Node](#5-search_node) — `pull_export.sh`, `search_service.py`, `static/index.html`

---

## 1. Root

### `config_loader.py`

```python
"""Single entry point for reading .config and creating Spark sessions.

Imported by every Python script in the repo.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _load() -> dict:
    """Parse ROOT/.config into a dict of strings.

    TODO:
      - skip blank lines and lines starting with '#'
      - split on the FIRST '=' only
      - strip surrounding quotes from the value
      - expand ${VAR} using keys already parsed above (same behaviour as bash `source`)
    """
    cfg: dict = {}
    # TODO: implement
    return cfg


CFG = _load()


def path(key: str) -> Path:
    """Return CFG[key] as an absolute Path (relative values resolve against ROOT)."""
    # TODO: implement
    raise NotImplementedError


def table(name: str | None = None) -> str:
    """Return fully-qualified Iceberg table name, e.g. 'lake.search.docs'.

    Uses ICEBERG_CATALOG, ICEBERG_NAMESPACE and `name` (default ICEBERG_DOCS_TABLE).
    """
    # TODO: implement
    raise NotImplementedError


def hdfs(key: str) -> str:
    """Return HDFS_URI + CFG[key], e.g. hdfs('HDFS_INDEX_DIR') -> 'hdfs://node2:9000/search_engine/index'."""
    # TODO: implement
    raise NotImplementedError


def build_spark(app: str, node: int):
    """Create a client-mode SparkSession whose driver lives on NODE{node}_HOST.

    TODO (all values from CFG):
      - .master(SPARK_MASTER_URL)
      - spark.driver.host = NODE{node}_HOST ; spark.driver.bindAddress = 0.0.0.0
      - spark.driver.port / spark.blockManager.port  (fixed, so executors can connect back)
      - spark.jars.packages = ICEBERG_PACKAGE
      - spark.sql.extensions = IcebergSparkSessionExtensions
      - catalog: spark.sql.catalog.<ICEBERG_CATALOG> = SparkCatalog, type=hadoop, warehouse=hdfs('HDFS_WAREHOUSE')
      - spark.hadoop.dfs.replication = HDFS_REPLICATION
      - spark.sql.shuffle.partitions, spark.executor.memory, spark.cores.max
      - spark.sparkContext.setCheckpointDir(hdfs('HDFS_CHECKPOINT_DIR'))
      - spark.sparkContext.setLogLevel('WARN')   # keep stdout clean
    """
    # TODO: implement
    raise NotImplementedError
```

Header used at the top of **every** Python script outside the root:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # repo root
from config_loader import CFG, build_spark, table, hdfs, path
```

Header used at the top of **every** shell script:

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }   # resolve relative config paths
```

---

## 2. Ingestion_Node

### `Ingestion_Node/upload_to_hdfs.sh`

```bash
# <standard shell header>
# Usage: upload_to_hdfs.sh <phase>
PHASE="${1:?usage: upload_to_hdfs.sh <phase>}"

# TODO: look up the filename via indirect expansion:
#   FILE_VAR="PHASE_${PHASE}_FILE";  FILE="${!FILE_VAR}"
# TODO: fail early with a clear message if the local file does not exist
#   (under "$(abs "$LOCAL_DATA_DIR")")
# TODO: ensure "$HDFS_URI$HDFS_RAW_DIR" exists   (hdfs dfs -mkdir -p)
# TODO: hdfs dfs -D dfs.replication="$HDFS_REPLICATION" -put -f <local> <hdfs raw dir>/<file>
# TODO: echo the final HDFS path so the caller/log shows what was uploaded
```

### `Ingestion_Node/run_ingestion.sh`

```bash
# <standard shell header>
# Usage: run_ingestion.sh <phase>
PHASE="${1:?usage: run_ingestion.sh <phase>}"

# TODO: call ./upload_to_hdfs.sh "$PHASE"
# TODO: python "$ROOT/Ingestion_Node/ingest.py" --phase "$PHASE"
# TODO: on success print the report path (REPORT_DIR/phase_<PHASE>.json)
```

### `Ingestion_Node/ingest.py`

```python
"""Node 1 — validate, deduplicate and MERGE one phase file into the Iceberg docs table.

Usage: python ingest.py --phase N
"""
import argparse
import json
# <standard python header>
from pyspark.sql import functions as F, Window as W

REQUIRED = CFG["REQUIRED_FIELDS"].split(",")

RAW_SCHEMA = ("doc_id string, url string, title string, text string, "
              "links array<string>, version bigint, op string, _corrupt string")


def parse_args():
    """--phase (int, required)."""
    # TODO: argparse
    raise NotImplementedError


def input_path(phase: int) -> str:
    """hdfs://.../raw/<PHASE_{phase}_FILE>"""
    # TODO: implement using hdfs('HDFS_RAW_DIR') and CFG[f"PHASE_{phase}_FILE"]
    raise NotImplementedError


def read_raw(spark, path_: str):
    """Read JSONL leniently.

    TODO:
      - spark.read.schema(RAW_SCHEMA), mode=PERMISSIVE, columnNameOfCorruptRecord='_corrupt'
      - .cache()   (required by Spark when filtering on the corrupt-record column)
      - op := upper(coalesce(op, 'UPSERT'))
      - add '_line' = monotonically_increasing_id() (tie-breaker for dedup)
    """
    raise NotImplementedError


def split_rejects(raw):
    """Return (ok_df, rejected_df, counts_dict).

    Reject when:
      - _corrupt is not null                      -> reason 'malformed_json'
      - op not in (UPSERT, DELETE)                -> reason 'bad_op'
      - doc_id blank                              -> reason 'missing_field'
      - op == UPSERT and any of REQUIRED blank    -> reason 'missing_field'
    TODO: add a 'reject_reason' column to rejected_df; counts_dict = {'malformed': n, 'missing_fields': n, ...}
    """
    raise NotImplementedError


def dedupe(ok_df):
    """Keep ONE row per doc_id: highest `version` wins (nulls last), later `_line` breaks ties.

    TODO: row_number() over Window.partitionBy('doc_id').orderBy(version desc_nulls_last, _line desc)
    Return (deduped_df, n_duplicates_dropped).
    """
    raise NotImplementedError


def stage(batch_df):
    """Select columns doc_id,url,title,text,links,version,op.

    TODO: links := array_distinct(coalesce(links, empty array<string>))
    Register as temp view 'staged'.
    """
    raise NotImplementedError


def latest_snapshot_id(spark):
    """Latest snapshot_id of table() or None if the table has no snapshots yet.

    TODO: query f"{table()}.snapshots" ordered by committed_at desc limit 1
    """
    raise NotImplementedError


def merge_batch(spark):
    """TODO: run the MERGE INTO from README Step 3(e):
        MATCHED AND op='DELETE'  -> DELETE
        MATCHED AND op='UPSERT'  -> UPDATE explicit columns
        NOT MATCHED AND op='UPSERT' -> INSERT explicit columns
    """
    raise NotImplementedError


def remove_dangling_links(spark) -> int:
    """Drop links pointing to doc_ids that no longer exist, over the WHOLE table.

    TODO (README Step 3(f)):
      - explode links -> left_semi join on valid doc_ids -> collect_list per doc
      - keep only rows whose link count changed
      - if any: MERGE ... WHEN MATCHED THEN UPDATE SET t.links = s.new_links
    Return number of docs modified (0 => no extra snapshot is created).
    """
    raise NotImplementedError


def diff_snapshots(spark, prev_id, new_id) -> dict:
    """Compare table contents at two snapshots.

    TODO:
      - read each with .option('snapshot-id', id).format('iceberg').load(table())
        (prev_id None -> treat as empty)
      - added   = doc_ids only in new
      - deleted = doc_ids only in prev
      - updated = doc_ids in both whose row hash differs
    """
    raise NotImplementedError


def write_report(phase: int, report: dict) -> Path:
    """Write REPORT_DIR/phase_<phase>.json (mkdir -p). Return the path."""
    # TODO: implement
    raise NotImplementedError


def main():
    args = parse_args()
    spark = build_spark(f"ingest-phase-{args.phase}", node=1)

    # TODO: raw = read_raw(...);  n_read = raw.count()
    # TODO: ok, rejected, counts = split_rejects(raw)
    # TODO: write rejected rows to hdfs('HDFS_REJECT_DIR') + f"/phase_{args.phase}"  (overwrite)
    # TODO: batch, n_dups = dedupe(ok);  stage(batch)
    # TODO: prev = latest_snapshot_id(spark)
    # TODO: merge_batch(spark)
    # TODO: n_links_fixed = remove_dangling_links(spark)
    # TODO: new = latest_snapshot_id(spark);  diff = diff_snapshots(spark, prev, new)
    # TODO: report = {phase, rows_read, malformed, missing_fields, duplicates_dropped,
    #                 accepted, dangling_links_fixed, prev_snapshot, new_snapshot, **diff}
    # TODO: print(json.dumps(report, indent=2));  write_report(...)
    spark.stop()


if __name__ == "__main__":
    main()
```

---

## 3. Cluster_1

### `Cluster_1/start_node.sh`

```bash
# <standard shell header>
# Node 2: HDFS NameNode + DataNode + Spark worker

# TODO: create local dirs if missing (NameNode/DataNode dirs from hdfs-site.xml, $(abs Cluster_1/work))
# TODO: if the NameNode dir is empty -> print "run: hdfs namenode -format" and exit 1 (never auto-format)
# TODO: hdfs --daemon start namenode ; hdfs --daemon start datanode
# TODO: wait until the NameNode answers (loop on `hdfs dfsadmin -safemode get`, with timeout)
# TODO: create HDFS layout (idempotent):
#   for d in RAW_DIR WAREHOUSE INDEX_DIR EXPORT_DIR REJECT_DIR CHECKPOINT_DIR; do
#     v=HDFS_$d; hdfs dfs -mkdir -p "$HDFS_URI${!v}"; done
# TODO: export SPARK_WORKER_DIR="$(abs Cluster_1/work)"
#       "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"
#       (the master must already be up on Node 3 -> print a hint if registration fails)
```

### `Cluster_1/stop_node.sh`

```bash
# <standard shell header>
# TODO: "$SPARK_HOME/sbin/stop-worker.sh"
# TODO: hdfs --daemon stop datanode ; hdfs --daemon stop namenode
```

### `Cluster_1/init_tables.py`

```python
"""Node 2 — create the Iceberg namespace and docs table (idempotent, run once)."""
# <standard python header>


def main():
    spark = build_spark("init-tables", node=2)
    # TODO: CREATE NAMESPACE IF NOT EXISTS <catalog>.<namespace>
    # TODO: CREATE TABLE IF NOT EXISTS table()
    #       (doc_id string, url string, title string, text string, links array<string>, version bigint)
    #       USING iceberg
    # TODO: print the table schema (DESCRIBE) as confirmation
    spark.stop()


if __name__ == "__main__":
    main()
```

### `Cluster_1/iceberg_admin.py`

```python
"""Node 2 — Iceberg maintenance CLI.

Usage:
  python iceberg_admin.py snapshots
  python iceberg_admin.py current-snapshot        # prints ONLY the id on stdout
  python iceberg_admin.py checksum [--snapshot ID]
  python iceberg_admin.py timetravel ID
  python iceberg_admin.py rollback ID
"""
import argparse
# <standard python header>
from pyspark.sql import functions as F


def read_at(spark, snapshot_id=None):
    """DataFrame of the docs table, at `snapshot_id` if given else current."""
    # TODO: spark.read.option('snapshot-id', id).format('iceberg').load(table())
    raise NotImplementedError


def cmd_snapshots(spark, args):
    """TODO: SELECT snapshot_id, committed_at, operation, summary FROM <table>.snapshots ORDER BY committed_at; show(truncate=False)"""


def cmd_current_snapshot(spark, args):
    """TODO: print only the latest snapshot_id (no extra text!)."""


def cmd_checksum(spark, args):
    """TODO: print 'count=<n> checksum=<k>' where
    k = sum(hash(doc_id, url, title, text, links, version)) at args.snapshot (or current)."""


def cmd_timetravel(spark, args):
    """TODO: read at args.id; print row count and 5 sample rows (doc_id, title)."""


def cmd_rollback(spark, args):
    """TODO: CALL <catalog>.system.rollback_to_snapshot('<namespace>.<table>', <id>);
    then print the new current snapshot id."""


def build_parser():
    """TODO: argparse subparsers for the 5 commands; checksum has optional --snapshot."""
    raise NotImplementedError


def main():
    args = build_parser().parse_args()
    spark = build_spark("iceberg-admin", node=2)
    # TODO: dispatch to cmd_* based on args.command
    spark.stop()


if __name__ == "__main__":
    main()
```

---

## 4. Cluster_2

### `Cluster_2/start_node.sh`

```bash
# <standard shell header>
# Node 3: Spark master + worker + (optional) HDFS DataNode

# TODO: "$SPARK_HOME/sbin/start-master.sh"
# TODO: export SPARK_WORKER_DIR="$(abs Cluster_2/work)"
#       "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"
# TODO: if [ "$CLUSTER2_RUN_DATANODE" = true ]; then hdfs --daemon start datanode; fi
# TODO: print Spark master UI URL (http://$NODE3_HOST:8080)
```

### `Cluster_2/stop_node.sh`

```bash
# <standard shell header>
# TODO: stop-worker.sh, stop-master.sh
# TODO: if [ "$CLUSTER2_RUN_DATANODE" = true ]; then hdfs --daemon stop datanode; fi
```

### `Cluster_2/build_index.py`

```python
"""Node 3 — build the distributed inverted index from a pinned Iceberg snapshot.

Usage: python build_index.py --snapshot ID
Output (HDFS_INDEX_DIR): postings(term, doc_id, tf), doc_stats(doc_id, doc_len)
"""
import argparse
# <standard python header>
from pyspark.sql import functions as F


def parse_args():
    """--snapshot (int, required)."""
    raise NotImplementedError


def read_docs(spark, snapshot_id):
    """TODO: read table() at snapshot via option('snapshot-id', ...). Select doc_id, title, text."""
    raise NotImplementedError


def tokenize(docs):
    """Return DF(doc_id, tokens array<string>).

    TODO: F.expr("regexp_extract_all(lower(concat_ws(' ', title, text)), '<TOKEN_REGEX>', 0)")
    The SAME regex + lowercase rule must be used by the search service.
    """
    raise NotImplementedError


def build_postings(tokens_df):
    """TODO: explode tokens -> groupBy(term, doc_id) -> count as tf. Return DF(term, doc_id, tf)."""
    raise NotImplementedError


def build_doc_stats(tokens_df):
    """TODO: DF(doc_id, doc_len=size(tokens))."""
    raise NotImplementedError


def main():
    args = parse_args()
    spark = build_spark("build-index", node=3)
    # TODO: docs -> tokens -> postings, doc_stats
    # TODO: write both with mode('overwrite').parquet(hdfs('HDFS_INDEX_DIR') + '/postings' | '/doc_stats')
    # TODO: print counts (docs, distinct terms, posting rows)
    spark.stop()


if __name__ == "__main__":
    main()
```

### `Cluster_2/pagerank.py`

```python
"""Node 3 — PageRank over the link graph of a pinned snapshot.

Usage: python pagerank.py --snapshot ID
Output (HDFS_INDEX_DIR/pagerank): doc_id, pagerank   (ranks sum to 1)
"""
import argparse
# <standard python header>
from pyspark.sql import functions as F

DAMPING = float(CFG["PAGERANK_DAMPING"])
ITERATIONS = int(CFG["PAGERANK_ITERATIONS"])    # spec minimum: 10


def parse_args():
    """--snapshot (int, required)."""
    raise NotImplementedError


def load_graph(spark, snapshot_id):
    """Return (nodes, edges, outdeg, links, sinks, N).

    TODO:
      - docs = read snapshot, select doc_id, links
      - nodes(id), edges(src,dst), outdeg(src,deg)
      - links = edges.join(outdeg, 'src').cache()
      - sinks = nodes left_anti outdeg   (pages with no out-links)
      - N = docs.count()
    """
    raise NotImplementedError


def iterate(nodes, links, sinks, ranks, N):
    """One PageRank iteration; return the new ranks DF(id, rank).

    TODO:
      - contribs: join links x ranks on src, c = rank/deg, groupBy dst, sum
      - sink_mass = sum of ranks of `sinks` (collected to driver)
      - rank = (1-d)/N + d*(coalesce(sum_contribs, 0) + sink_mass/N)
      - .checkpoint()   # reliable HDFS checkpoint: cuts lineage AND supports fault tolerance
    """
    raise NotImplementedError


def main():
    args = parse_args()
    spark = build_spark("pagerank", node=3)
    # TODO: load_graph; ranks = nodes with rank = 1/N
    # TODO: for i in range(ITERATIONS): ranks = iterate(...); log iteration number + sum(rank) to stderr
    # TODO: assert abs(total - 1.0) < 1e-6
    # TODO: write (doc_id, pagerank) to hdfs('HDFS_INDEX_DIR') + '/pagerank' (overwrite)
    spark.stop()


if __name__ == "__main__":
    main()
```

### `Cluster_2/export.py`

```python
"""Node 3 — validate the index and export plain Parquet for serving.

Usage: python export.py --snapshot ID --version N
Writes hdfs://.../export/v<N>.tmp/{postings,docs,manifest}   (run_pipeline.sh renames to v<N>)
Exit code != 0 on ANY validation failure; nothing is published in that case.
"""
import argparse
from datetime import datetime, timezone
# <standard python header>
from pyspark.sql import functions as F


def parse_args():
    """--snapshot (int), --version (int); both required."""
    raise NotImplementedError


def load_inputs(spark, snapshot_id):
    """Return (postings, doc_stats, pagerank, docs_meta).

    TODO: postings/doc_stats/pagerank from hdfs('HDFS_INDEX_DIR'); docs_meta(doc_id,url,title)
          from the Iceberg snapshot.
    """
    raise NotImplementedError


def build_docs_export(docs_meta, doc_stats, pagerank):
    """Join into DF(doc_id, url, title, doc_len, pagerank)."""
    raise NotImplementedError


def validate(postings, docs_export, pagerank):
    """Raise ValueError with a clear message on the first failure.

    Checks:
      - docs_export.count() > 0
      - pagerank.count() == docs_export.count()
      - abs(sum(pagerank) - 1) < 1e-6
      - no posting doc_id missing from docs_export   (left_anti must be empty)
      - no null/blank url or title in docs_export
    """
    raise NotImplementedError


def write_export(spark, version, snapshot_id, postings, docs_export):
    """TODO:
      - base = hdfs('HDFS_EXPORT_DIR') + f'/v{version}.tmp'
      - postings.coalesce(1).write.mode('overwrite').parquet(base + '/postings')
      - docs_export.coalesce(1).write.mode('overwrite').parquet(base + '/docs')
      - manifest (1 row): version, snapshot_id, n_docs, n_postings, created_at (UTC ISO)
        -> .coalesce(1).write.parquet(base + '/manifest')
    """
    raise NotImplementedError


def main():
    args = parse_args()
    spark = build_spark("export", node=3)
    # TODO: load_inputs -> build_docs_export -> validate -> write_export
    # TODO: print 'EXPORT OK v<N> docs=<n> postings=<n>'
    spark.stop()


if __name__ == "__main__":
    main()
```

### `Cluster_2/run_pipeline.sh`

```bash
# <standard shell header>
# Usage: run_pipeline.sh <version>      (version == phase number)
VERSION="${1:?usage: run_pipeline.sh <version>}"

# TODO: SNAP=$(python "$ROOT/Cluster_1/iceberg_admin.py" current-snapshot)
#       -> every job below reads this SAME snapshot (consistency across stages)
# TODO: time each stage (start=$(date +%s) ... echo "index: $((end-start))s") for the report
# TODO: python "$ROOT/Cluster_2/build_index.py" --snapshot "$SNAP"
# TODO: python "$ROOT/Cluster_2/pagerank.py"    --snapshot "$SNAP"
# TODO: python "$ROOT/Cluster_2/export.py"      --snapshot "$SNAP" --version "$VERSION"
#       (set -e stops the script here if validation fails -> nothing is published)
#
# TODO: atomic publish in HDFS
#   E="$HDFS_URI$HDFS_EXPORT_DIR"
#   hdfs dfs -rm -r -f "$E/v$VERSION"
#   hdfs dfs -mv "$E/v$VERSION.tmp" "$E/v$VERSION"
#
# TODO: stage a local copy for Node 4 to pull
#   S="$(abs "$STAGING_DIR")"; mkdir -p "$S"; rm -rf "$S/v$VERSION"
#   hdfs dfs -get "$E/v$VERSION" "$S/v$VERSION"
# TODO: echo "READY: $S/v$VERSION"
```

---

## 5. Search_Node

### `Search_Node/pull_export.sh`

```bash
# <standard shell header>
# Usage: pull_export.sh <version>
VERSION="${1:?usage: pull_export.sh <version>}"
SERVING="$(abs "$SERVING_DIR")"; mkdir -p "$SERVING"

# TODO: decide local vs remote:
#   local  -> NODE3_HOST resolves to a loopback/own address  -> cp -r "$(abs "$STAGING_DIR")/v$VERSION/." "$SERVING/v$VERSION.tmp/"
#   remote -> rsync -az --delete "$SSH_USER@$NODE3_HOST:$ROOT/Cluster_2/export_staging/v$VERSION/" "$SERVING/v$VERSION.tmp/"
#   (rm -rf the .tmp dir first)
#
# TODO: quick completeness check: _SUCCESS must exist in postings/, docs/, manifest/
#       if not -> rm -rf .tmp ; echo error ; exit 1
#
# TODO: rm -rf "$SERVING/v$VERSION"; mv "$SERVING/v$VERSION.tmp" "$SERVING/v$VERSION"
#
# TODO: ask the running service to load it
#   CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:$SEARCH_PORT/reload?version=$VERSION" || true)
#   case "$CODE" in
#     200) ln -sfn "v$VERSION" "$SERVING/current" ; echo "switched to v$VERSION" ;;
#     000) ln -sfn "v$VERSION" "$SERVING/current" ; echo "service not running; start it to serve v$VERSION" ;;
#     *)   mv "$SERVING/v$VERSION" "$SERVING/v$VERSION.rejected" ; echo "REJECTED (HTTP $CODE); still serving old version" ; exit 1 ;;
#   esac
```

### `Search_Node/search_service.py`

```python
"""Node 4 — FastAPI search service (BM25 + PageRank), no Spark/HDFS dependency.

Run: uvicorn search_service:app --app-dir Search_Node --host 0.0.0.0 --port $SEARCH_PORT
"""
import math
import re
import threading
from dataclasses import dataclass
# <standard python header>        # only CFG and path are needed here
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

SERVING = path("SERVING_DIR")
K1, B = float(CFG["BM25_K1"]), float(CFG["BM25_B"])
W_BM25, W_PR = float(CFG["W_BM25"]), float(CFG["W_PAGERANK"])
TOKEN_RE = re.compile(CFG["TOKEN_REGEX"])
DEFAULT_K = int(CFG["TOP_K"])

app = FastAPI(title="Mini Search Engine")
_lock = threading.Lock()
STATE: "IndexState | None" = None     # swapped atomically; never mutated in place


@dataclass(frozen=True)
class IndexState:
    version: int
    n_docs: int
    avgdl: float
    doc_len: np.ndarray        # per doc index
    pagerank: np.ndarray       # per doc index
    max_pr: float              # global max PageRank
    doc_ids: list
    urls: list
    titles: list
    inv: dict                  # term -> (doc_idx np.ndarray, tf np.ndarray[float])
    manifest: dict


def tokenize(text: str) -> list[str]:
    """lowercase + TOKEN_RE.findall — MUST match the Spark index job."""
    # TODO: implement
    raise NotImplementedError


def load_version(version: int) -> IndexState:
    """Load and validate SERVING/v<version>; raise ValueError on ANY problem.

    TODO:
      - for n in (postings, docs, manifest): require <dir>/<n>/_SUCCESS
      - pd.read_parquet each dataset
      - len(docs) == manifest.n_docs and len(postings) == manifest.n_postings
      - doc_id -> row index map; map postings.doc_id to indices
      - inv = {term: (idx_array, tf_array)} via postings.groupby('term')
      - avgdl = mean(doc_len); max_pr = max(pagerank)
      - return IndexState(...)
    """
    raise NotImplementedError


def search(state: IndexState, q: str, k: int) -> list[dict]:
    """Rank documents for query `q`.

    TODO:
      - terms = set(tokenize(q)); skip terms not in state.inv
      - for each term: df = len(idx); idf = ln(1 + (N - df + 0.5)/(df + 0.5))   # or the course snippet's IDF
          bm25_part = idf * tf*(K1+1) / (tf + K1*(1 - B + B*doc_len[idx]/avgdl))
          accumulate into scores[doc_idx]
      - no matches -> []
      - max_bm25 = max(scores.values())
      - final = W_BM25 * bm25/max_bm25 + W_PR * pagerank[i]/max_pr
      - sort desc, take k; return dicts:
        {rank, doc_id, url, title, bm25, pagerank, score}
    """
    raise NotImplementedError


def _current_version_from_symlink() -> int | None:
    """TODO: resolve SERVING/'current' -> 'v<N>' -> N ; None if missing."""
    raise NotImplementedError


@app.on_event("startup")
def startup():
    """TODO: v = _current_version_from_symlink(); if v is not None, STATE = load_version(v).
    A failed load should be logged, not crash the server."""


@app.get("/search")
def search_endpoint(q: str = Query(..., min_length=1), k: int = DEFAULT_K):
    """TODO: 503 if STATE is None; else {'version': STATE.version, 'query': q, 'results': search(STATE, q, k)}"""
    raise NotImplementedError


@app.get("/version")
def version_endpoint():
    """TODO: 503 if STATE is None; else {'version': ..., 'manifest': ...}"""
    raise NotImplementedError


@app.post("/reload")
def reload_endpoint(version: int | None = None):
    """TODO:
      - version None -> highest N among SERVING/v<N> directories
      - try: new = load_version(version)
        except Exception as e: raise HTTPException(400, detail=str(e))    # STATE untouched
      - with _lock: STATE = new       (global)
      - return {'status': 'ok', 'version': version}
    """
    raise NotImplementedError


@app.get("/")
def index():
    """Serve static/index.html."""
    # TODO: FileResponse(Path(__file__).parent / 'static' / 'index.html')
    raise NotImplementedError
```

### `Search_Node/static/index.html`

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Mini Search</title>
  <!-- TODO: minimal CSS (centered search box, simple results table) -->
</head>
<body>
  <h1>Mini Search</h1>
  <small id="version"><!-- TODO: filled from GET /version, e.g. "index v1 · 1,023 docs" --></small>

  <!-- TODO: <input id="q"> + <button id="go">; submit on Enter -->

  <table id="results">
    <!-- TODO: header row: # | Title | Doc ID | URL | Score -->
  </table>
  <p id="msg"><!-- TODO: "No results" / error text (503 -> "index not loaded") --></p>

  <script>
    async function loadVersion() {
      // TODO: fetch('/version') -> show version + n_docs in #version
    }
    async function doSearch() {
      // TODO: fetch('/search?q=' + encodeURIComponent(q)) -> render rows
      //       (URL as a clickable link, scores to 3-4 decimals; escape text before inserting into HTML)
    }
    // TODO: wire up button click + Enter key, call loadVersion() on page load
  </script>
</body>
</html>
```

---

## Suggested Build Order

1. `config_loader.py` — test with `python -c "from config_loader import CFG; print(CFG['SPARK_MASTER_URL'])"`.
2. `Cluster_1/start_node.sh`, `Cluster_2/start_node.sh` → confirm HDFS + 2 Spark workers.
3. `init_tables.py` → `ingest.py` (+ shell wrappers) → check the Phase 0 report.
4. `iceberg_admin.py` (needed by `run_pipeline.sh`).
5. `build_index.py` → `pagerank.py` → `export.py` → `run_pipeline.sh`.
6. `pull_export.sh` + `search_service.py` + `index.html`.
7. Phase 1, then the Step 9 demonstrations from `README.md`.
