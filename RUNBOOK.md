# Distributed Search Engine Runbook

This runbook describes the complete execution order for the project in two
deployment modes:

1. **Local mode**: one machine runs Hadoop, HDFS, Spark, ingestion, indexing,
   PageRank, export, and the FastAPI service.
2. **Four-VM mode**: the four logical nodes run across four machines, using
   `node1` through `node4` as host aliases.

The application code is shared between both modes. The intended configuration
change when moving between modes is limited to the environment-specific values
at the top of `.config`, plus the machine networking and service setup described
below.

All commands in this document are run from the repository root unless stated
otherwise.

---

## 1. Local setup and execution

### 1.1 Local topology

In local mode, all logical nodes are on one machine:

| Logical node | Local responsibility |
|---|---|
| Node 1 | Uploads raw JSONL and runs ingestion |
| Node 2 | Runs the NameNode/DataNode and Iceberg administration |
| Node 3 | Runs Spark indexing, PageRank, and export |
| Node 4 | Runs FastAPI and serves the exported index |

You may run Spark jobs in either of these ways:

- **Recommended for a single-machine test**: Spark `local[2]` mode. This does
  not require a Spark standalone master.
- **Optional local cluster mode**: start the Spark master and two local workers
  using the `Cluster_1` and `Cluster_2` scripts.

The steps below use local Spark mode because it is deterministic and does not
require local host aliases such as `node2` or `node3`.

### 1.2 Local prerequisites

Install or verify the following:

- Java compatible with the installed Spark distribution.
- Hadoop/HDFS client commands: `hdfs`.
- A running local NameNode and DataNode.
- A Python virtual environment containing the dependencies.
- A matching Spark/PySpark and Iceberg runtime combination.

For the repository virtual environment:

```bash
source .venv/bin/activate
python --version
python -c "import pyspark; print(pyspark.__version__)"
python -c "import fastapi, pandas, pyarrow, numpy; print('Node 4 dependencies OK')"
```

The Python version used by Spark drivers and executors must match. If the
installed Spark launcher is a different version from the `.venv` PySpark
package, use the matching Spark launcher explicitly:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

The Iceberg package in `.config` must match the Spark and Scala versions. For
example, Spark 3.3 with Scala 2.12 requires:

```text
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2
```

### 1.3 Local Hadoop verification

Start Hadoop using the installation-specific commands if it is not already
running. A typical setup is:

```bash
start-dfs.sh
start-yarn.sh
```

Verify the daemons and HDFS:

```bash
jps
hdfs dfsadmin -report
yarn node -list
hdfs getconf -confKey fs.defaultFS
```

The local HDFS URI must match the actual Hadoop configuration. A common local
value is:

```text
hdfs://localhost:9000
```

If `.config` contains `hdfs://node2:9000` but the local Hadoop installation
reports `hdfs://localhost:9000`, do not edit application code. Override the
existing configuration key for the current shell:

```bash
export HDFS_URI=hdfs://localhost:9000
```

The project supports environment overrides for existing `.config` keys. These
overrides do not add configuration keys and do not modify the file.

### 1.4 Local Spark environment overrides

For local Spark mode, export the following values before running any Python
stage:

```bash
export SPARK_MASTER_URL='local[2]'
export HDFS_URI='hdfs://localhost:9000'
export NODE1_HOST='127.0.0.1'
export NODE2_HOST='127.0.0.1'
export NODE3_HOST='127.0.0.1'
export NODE4_HOST='127.0.0.1'
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

These settings are required because the production `.config` uses `node1`
through `node4` and a Spark standalone URL. They are local-test overrides only.

### 1.5 Create the HDFS directories

The configured HDFS directories are:

- `/search_engine/raw`
- `/search_engine/warehouse`
- `/search_engine/index`
- `/search_engine/export`
- `/search_engine/rejected`
- `/search_engine/checkpoints`

Create them idempotently:

```bash
for d in RAW_DIR WAREHOUSE INDEX_DIR EXPORT_DIR REJECT_DIR CHECKPOINT_DIR; do
  var="HDFS_$d"
  hdfs dfs -mkdir -p "$HDFS_URI${!var}"
done
```

### 1.6 Profile the input data

The configured files are:

- `Ingestion_Node/data/pages.jsonl`: Phase 0 baseline corpus.
- `Ingestion_Node/data/changes.jsonl`: Phase 1 changes.
- `Ingestion_Node/data/bad_changes.jsonl`: optional Phase 2 rollback demo.

Check line counts and JSON validity:

```bash
wc -l Ingestion_Node/data/pages.jsonl \
      Ingestion_Node/data/changes.jsonl

for f in Ingestion_Node/data/*.jsonl; do
  bad=0
  while IFS= read -r line; do
    if ! printf '%s\n' "$line" | jq -e . >/dev/null 2>&1; then
      bad=$((bad + 1))
    fi
  done < "$f"
  echo "$f malformed=$bad"
done
```

Inspect keys and operations:

```bash
jq -R -c 'fromjson? // {malformed: true}' Ingestion_Node/data/pages.jsonl \
  | jq -s 'map(select(.malformed == true)) | length'

jq -c 'keys' Ingestion_Node/data/pages.jsonl | sort | uniq -c
jq -c 'keys' Ingestion_Node/data/changes.jsonl | sort | uniq -c
jq -r '.op // "UPSERT"' Ingestion_Node/data/changes.jsonl | sort | uniq -c
jq -r '.doc_id // empty' Ingestion_Node/data/pages.jsonl \
  | sort | uniq -d | head
```

The ingestion report is the authoritative result after processing. It records
malformed rows, missing fields, duplicates, accepted rows, dangling-link
repairs, and snapshot differences.

### 1.7 Initialize the Iceberg table

The table must be initialized once before the first ingestion:

```bash
PKG="$(grep '^ICEBERG_PACKAGE=' .config | cut -d= -f2-)"
/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_1/init_tables.py
```

If the Spark launcher is already compatible with the activated virtual
environment, the plain Python form may be used instead:

```bash
python Cluster_1/init_tables.py
```

The command creates:

```text
lake.search.docs
```

The Hadoop catalog stores the table metadata and data under the configured
HDFS warehouse.

### 1.8 Run Phase 0 ingestion

Upload the raw file to HDFS:

```bash
Ingestion_Node/upload_to_hdfs.sh 0
```

Run ingestion directly with the matching Spark launcher:

```bash
/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Ingestion_Node/ingest.py --phase 0
```

The ingestion stage:

1. Reads JSON permissively.
2. Rejects malformed JSON and invalid records.
3. Deduplicates by `doc_id`.
4. Applies the highest-version/later-line rule.
5. Merges UPSERT and DELETE records into Iceberg.
6. Removes links to missing documents.
7. Writes a report under `Ingestion_Node/reports/`.

Check the result:

```bash
cat Ingestion_Node/reports/phase_0.json

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_1/iceberg_admin.py snapshots

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_1/iceberg_admin.py current-snapshot

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_1/iceberg_admin.py checksum
```

Save the Phase 0 checksum as `CHK0`.

### 1.9 Build the Phase 0 index and PageRank

Read the current snapshot once:

```bash
SNAP=$(/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_1/iceberg_admin.py current-snapshot | tail -1)
echo "$SNAP"
```

Use that same snapshot for all three stages:

```bash
/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_2/build_index.py --snapshot "$SNAP"

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_2/pagerank.py --snapshot "$SNAP"

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_2/export.py --snapshot "$SNAP" --version 0
```

Expected output includes:

```text
EXPORT OK v0 docs=<count> postings=<count>
```

The export is initially written to:

```text
hdfs://.../search_engine/export/v0.tmp
```

Publish it atomically:

```bash
E="$HDFS_URI$HDFS_EXPORT_DIR"
hdfs dfs -rm -r -f "$E/v0"
hdfs dfs -mv "$E/v0.tmp" "$E/v0"

rm -rf Cluster_2/export_staging/v0
mkdir -p Cluster_2/export_staging
hdfs dfs -get "$E/v0" Cluster_2/export_staging/v0
```

### 1.10 Start the local search service

Pull the staged export into the serving directory:

```bash
Search_Node/pull_export.sh 0
```

If the service is not running, the script prints a service-not-running
message and still creates:

```text
Search_Node/serving/v0
Search_Node/serving/current -> v0
```

Start FastAPI:

```bash
python -m uvicorn Search_Node.search_service:app \
  --host 127.0.0.1 \
  --port "$SEARCH_PORT"
```

Test the service from another terminal:

```bash
curl http://127.0.0.1:8000/version
curl --get --data-urlencode 'q=April month' \
  'http://127.0.0.1:8000/search?k=5'
curl http://127.0.0.1:8000/
```

Open `http://127.0.0.1:8000/` in a browser to use the frontend.

### 1.11 Run Phase 1

Upload and ingest the changes:

```bash
Ingestion_Node/upload_to_hdfs.sh 1

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Ingestion_Node/ingest.py --phase 1
```

Read the new snapshot and rebuild every derived dataset from that same
snapshot:

```bash
SNAP=$(/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_1/iceberg_admin.py current-snapshot | tail -1)

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_2/build_index.py --snapshot "$SNAP"

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_2/pagerank.py --snapshot "$SNAP"

/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages "$PKG" \
  Cluster_2/export.py --snapshot "$SNAP" --version 1
```

Publish and stage version 1:

```bash
E="$HDFS_URI$HDFS_EXPORT_DIR"
hdfs dfs -rm -r -f "$E/v1"
hdfs dfs -mv "$E/v1.tmp" "$E/v1"

rm -rf Cluster_2/export_staging/v1
hdfs dfs -get "$E/v1" Cluster_2/export_staging/v1
Search_Node/pull_export.sh 1
```

Because the service is running, `pull_export.sh` calls `/reload`. A successful
HTTP 200 response updates `serving/current` to `v1`.

Verify:

```bash
curl http://127.0.0.1:8000/version
curl --get --data-urlencode 'q=added during project phase 1' \
  'http://127.0.0.1:8000/search?k=5'
curl -X POST 'http://127.0.0.1:8000/reload?version=0'
```

### 1.12 Local validation and recovery demonstrations

#### Idempotent ingestion

Save the current checksum and snapshot:

```bash
CHK1=$(/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Cluster_1/iceberg_admin.py checksum | tail -1)
SNAP1=$(/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Cluster_1/iceberg_admin.py current-snapshot | tail -1)
```

Re-ingest the same phase:

```bash
/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Ingestion_Node/ingest.py --phase 1
```

Confirm both checksum and snapshot are unchanged:

```bash
/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Cluster_1/iceberg_admin.py checksum
/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Cluster_1/iceberg_admin.py current-snapshot
```

#### Time travel

```bash
/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Cluster_1/iceberg_admin.py timetravel "$SNAP"
```

#### Failed export and failed reload

Remove `_SUCCESS` from a copied local serving export:

```bash
cp -a Search_Node/serving/v1 Search_Node/serving/v2
rm Search_Node/serving/v2/postings/_SUCCESS
Search_Node/pull_export.sh 2
```

The command must fail, and `Search_Node/serving/current` must still point to
the previously active version.

#### Rollback

The FastAPI rollback is an in-memory serving rollback:

```bash
curl -X POST 'http://127.0.0.1:8000/reload?version=0'
```

The Iceberg rollback is a table rollback:

```bash
/opt/spark/bin/spark-submit --master 'local[2]' --packages "$PKG" \
  Cluster_1/iceberg_admin.py rollback "$SNAP0"
```

After an Iceberg rollback, rebuild the index, PageRank, and export from the
new current snapshot before serving that state.

---

## 2. Four-VM setup and execution

### 2.1 Four-VM topology

Use one repository clone at the same absolute path on every VM:

| Node | Host alias | Runs |
|---|---|---|
| Node 1 | `node1` | ingestion driver and HDFS client |
| Node 2 | `node2` | HDFS NameNode, DataNode, Spark worker, Iceberg administration |
| Node 3 | `node3` | Spark master, Spark worker, optional DataNode, pipeline drivers |
| Node 4 | `node4` | FastAPI service and frontend |

The scripts must be run from the same repository-relative layout on every VM.
In particular, Node 4's `rsync` operation expects the repository to have the
same absolute path on Node 3.

### 2.2 Network and host configuration

On every VM, define all aliases in `/etc/hosts`:

```text
<node1-ip> node1
<node2-ip> node2
<node3-ip> node3
<node4-ip> node4
```

Use the real ZeroTier or private network addresses. Do not use loopback
addresses for the four-VM setup.

Verify from every VM:

```bash
getent hosts node1 node2 node3 node4
ping -c 1 node2
ping -c 1 node3
```

Open or allow these ports between the VMs as applicable:

- HDFS NameNode RPC: `9000`
- HDFS NameNode web UI: `9870`
- HDFS DataNode ports: `9864`, `9866`, `9867`
- Spark master: `7077`
- Spark master/worker UIs: `8080`, `8081`
- Spark driver UI: `4040`
- Spark driver and block manager: `40000`, `40001`
- Search service: `8000`
- SSH/rsync: `22`

### 2.3 Install software on each VM

Nodes 1-3 need compatible Hadoop and Spark client/runtime installations:

- Java
- Hadoop client binaries
- Spark binaries
- Matching PySpark version
- Matching Python minor version on all Spark machines

Node 4 needs:

- Python
- FastAPI
- Uvicorn
- Pandas
- PyArrow
- NumPy
- `curl`
- `rsync`

Install project Python dependencies:

```bash
# Nodes 1-3
python -m pip install -r Ingestion_Node/requirements.txt

# Node 4
python -m pip install -r Search_Node/requirements.txt
```

Set these variables on Nodes 1-3:

```bash
export JAVA_HOME=/path/to/java
export HADOOP_HOME=/path/to/hadoop
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME=/path/to/spark
export PYSPARK_PYTHON=/path/to/shared/python
export PYSPARK_DRIVER_PYTHON=/path/to/shared/python
```

The Python path must resolve to the same compatible Python installation on
Nodes 1, 2, and 3. Node 4 does not need PySpark.

### 2.4 Configure Hadoop

On Node 2, configure the NameNode and DataNode. On Node 3, install the same
Hadoop configuration files required for a DataNode and HDFS client.

Important properties include:

```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://node2:9000</value>
</property>
<property>
  <name>dfs.namenode.rpc-bind-host</name>
  <value>0.0.0.0</value>
</property>
<property>
  <name>dfs.datanode.use.datanode.hostname</name>
  <value>true</value>
</property>
<property>
  <name>dfs.client.use.datanode.hostname</name>
  <value>true</value>
</property>
<property>
  <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
  <value>false</value>
</property>
```

Set persistent local directories for:

- `dfs.namenode.name.dir` on Node 2
- `dfs.datanode.data.dir` on Node 2 and Node 3

Format the NameNode exactly once, and only on Node 2:

```bash
hdfs namenode -format
```

Never run this command again against an existing cluster unless you explicitly
intend to destroy the HDFS namespace.

### 2.5 Configure Spark

On Node 3, configure the Spark master host:

```bash
export SPARK_MASTER_HOST=node3
```

On Node 2 and Node 3, set the local network address used by Spark workers:

```bash
export SPARK_LOCAL_IP=node2   # Node 2
export SPARK_LOCAL_IP=node3   # Node 3
```

The `.config` values used for the four-VM deployment are:

```text
NODE1_HOST=node1
NODE2_HOST=node2
NODE3_HOST=node3
NODE4_HOST=node4
HDFS_URI=hdfs://node2:9000
SPARK_MASTER_URL=spark://node3:7077
```

The driver ports in `.config` must be reachable from the worker machines:

```text
SPARK_DRIVER_PORT=40000
SPARK_BLOCKMANAGER_PORT=40001
```

### 2.6 Configure SSH from Node 4 to Node 3

The export pull script uses `rsync` in four-VM mode. Set up passwordless SSH
from Node 4 to Node 3:

```bash
# On Node 4
ssh-keygen -t ed25519
ssh-copy-id "$SSH_USER@node3"
ssh "$SSH_USER@node3" hostname
```

The repository path on Node 3 must match `ROOT` on Node 4, for example:

```text
/home/student/search-engine
```

### 2.7 Change `.config` from local to four-VM mode

Only change the environment-specific values:

```diff
-HDFS_REPLICATION=1
-CLUSTER2_RUN_DATANODE=false
-SSH_USER=youruser
+HDFS_REPLICATION=2
+CLUSTER2_RUN_DATANODE=true
+SSH_USER=<real-node4-user>
```

Keep the topology values as aliases:

```text
NODE1_HOST=node1
NODE2_HOST=node2
NODE3_HOST=node3
NODE4_HOST=node4
```

Do not replace aliases with IP addresses in scripts or application code.
Copy the updated `.config` to all four repository clones.

The other local-to-VM changes are environmental, not code changes:

| Setting | Local | Four VMs |
|---|---|---|
| `HDFS_REPLICATION` | `1` | `2` |
| `CLUSTER2_RUN_DATANODE` | `false` | `true` |
| `SSH_USER` | unused | real Node 3 SSH user |
| `HDFS_URI` | commonly `hdfs://localhost:9000` override | `hdfs://node2:9000` |
| `SPARK_MASTER_URL` | commonly `local[2]` override | `spark://node3:7077` |
| host aliases | loopback/local overrides | real `/etc/hosts` mappings |
| export transfer | local copy | `rsync` over SSH |

Do not leave local shell overrides such as `SPARK_MASTER_URL=local[2]` or
`HDFS_URI=hdfs://localhost:9000` exported when starting the four-VM services.

### 2.8 Start the four-VM cluster

Start services in this order.

#### Node 2: NameNode, DataNode, and Spark worker

```bash
Cluster_1/start_node.sh
```

This starts the NameNode and Node 2 DataNode, creates the HDFS project
directories, and starts the Node 2 Spark worker.

#### Node 3: Spark master, worker, and optional DataNode

```bash
Cluster_2/start_node.sh
```

Because `CLUSTER2_RUN_DATANODE=true`, this also starts the Node 3 DataNode.

Verify from Node 2 or Node 3:

```bash
hdfs dfsadmin -report
```

Expected result: two live DataNodes.

Verify the Spark master UI at:

```text
http://node3:8080
```

Expected result: two registered workers.

### 2.9 Initialize the table on Node 2

Run once:

```bash
python Cluster_1/init_tables.py
```

If the Python launcher does not match the Spark installation, use the matching
Spark launcher and package explicitly:

```bash
spark-submit \
  --master "$SPARK_MASTER_URL" \
  --packages "$ICEBERG_PACKAGE" \
  Cluster_1/init_tables.py
```

### 2.10 Run ingestion on Node 1

From Node 1:

```bash
Ingestion_Node/run_ingestion.sh 0
```

This uploads `PHASE_0_FILE` to HDFS and runs the ingestion driver. Verify:

```bash
cat Ingestion_Node/reports/phase_0.json
```

For Phase 1:

```bash
Ingestion_Node/run_ingestion.sh 1
cat Ingestion_Node/reports/phase_1.json
```

### 2.11 Run the pipeline on Node 3

From Node 3:

```bash
Cluster_2/run_pipeline.sh 0
```

The script:

1. Reads one current Iceberg snapshot.
2. Builds the inverted index from that pinned snapshot.
3. Runs PageRank for the configured number of iterations.
4. Validates and writes the export to `v0.tmp`.
5. Atomically renames it to `v0` in HDFS.
6. Copies it to `Cluster_2/export_staging/v0`.

For Phase 1:

```bash
Cluster_2/run_pipeline.sh 1
```

Do not run index, PageRank, and export manually with different snapshot IDs.
The pipeline script is specifically responsible for snapshot consistency.

### 2.12 Run the search service on Node 4

Start the service on Node 4:

```bash
python -m uvicorn Search_Node.search_service:app \
  --host 0.0.0.0 \
  --port "$SEARCH_PORT"
```

Pull the Phase 0 export:

```bash
Search_Node/pull_export.sh 0
```

The script uses local copying only when it detects local mode. In four-VM mode
it uses:

```text
rsync -az --delete $SSH_USER@node3:<repo-path>/Cluster_2/export_staging/v0/
```

After the service accepts the export with HTTP 200, the script updates:

```text
Search_Node/serving/current -> v0
```

For Phase 1:

```bash
Search_Node/pull_export.sh 1
```

Verify from Node 4 or another permitted client:

```bash
curl http://node4:8000/version
curl --get --data-urlencode 'q=added during project phase 1' \
  'http://node4:8000/search?k=5'
```

### 2.13 Four-VM validation demonstrations

Run the same demonstrations as local mode, with the following additional
cluster checks.

#### Spark worker resilience

While a long PageRank job is running, stop one worker process on Node 2 or
Node 3. Spark should retry lost work on the remaining worker:

```bash
jps
kill -9 <specific-worker-pid>
```

Restart the affected worker with the appropriate node start script and verify
that it registers again in the Spark master UI.

#### HDFS replication

Check block locations:

```bash
hdfs fsck /search_engine -files -blocks -locations
```

With `HDFS_REPLICATION=2`, blocks should have two replicas when both DataNodes
are healthy. Temporarily stopping one DataNode should leave data readable while
showing under-replication. Restarting it should allow HDFS to recover replicas.

#### Failed export

Remove `_SUCCESS` from a staged export on Node 4 or corrupt one of its datasets:

```bash
rm Search_Node/serving/v1/postings/_SUCCESS
Search_Node/pull_export.sh 1
```

The pull must fail without changing the previously served `current` symlink.

---

## 3. Switching between local and four-VM modes

### Local -> four VMs

1. Stop local services if they are running.
2. Ensure the repository exists at the same absolute path on all VMs.
3. Configure `/etc/hosts` with real `node1`-`node4` addresses on every VM.
4. Configure Hadoop on Nodes 2 and 3 to use `hdfs://node2:9000`.
5. Configure Spark master/worker networking.
6. Set `HDFS_REPLICATION=2`.
7. Set `CLUSTER2_RUN_DATANODE=true`.
8. Set `SSH_USER` to the Node 3 account used by Node 4.
9. Copy the updated `.config` to every VM.
10. Remove local shell overrides from the environment.
11. Format the NameNode only if this is a new cluster.
12. Start Node 2, then Node 3.
13. Verify two DataNodes and two Spark workers.
14. Run initialization, ingestion, pipeline, and serving in node order.

Do not copy local `Search_Node/serving` state as a substitute for running the
four-VM export pipeline. Rebuild and publish from the four-VM Iceberg snapshot.

### Four VMs -> local

1. Stop the four-VM Node 2 and Node 3 services if they are no longer needed.
2. Restore local Hadoop configuration with `fs.defaultFS=hdfs://localhost:9000`.
3. Set:

   ```bash
   export SPARK_MASTER_URL='local[2]'
   export HDFS_URI='hdfs://localhost:9000'
   export NODE1_HOST='127.0.0.1'
   export NODE2_HOST='127.0.0.1'
   export NODE3_HOST='127.0.0.1'
   export NODE4_HOST='127.0.0.1'
   ```

4. Set `HDFS_REPLICATION=1`.
5. Set `CLUSTER2_RUN_DATANODE=false`.
6. `SSH_USER` is ignored in local mode.
7. Do not run `rsync`; `pull_export.sh` should use the local staging directory.
8. Ensure local HDFS is running and create the project directories.
9. Reinitialize or reuse the local Iceberg warehouse as appropriate.
10. Run the local sequence from Section 1.

Local and four-VM HDFS namespaces are separate deployments. A table and
snapshot created in one deployment are not automatically available in the
other. Treat the first local or four-VM run as a fresh environment unless the
HDFS warehouse has intentionally been preserved.

---

## 4. Operational safety rules

- Never run `hdfs namenode -format` on an existing cluster.
- Never delete paths outside the configured HDFS base, staging directory, or
  serving directory.
- Always use the same snapshot for index, PageRank, and export.
- Keep `current` pointed at the last validated serving version.
- If export validation fails, do not manually rename the temporary export.
- If `/reload` returns anything other than HTTP 200, keep serving the previous
  version and investigate the error.
- Use `iceberg_admin.py checksum` before and after repeat ingestion to prove
  idempotency.
- Keep Spark, Scala, Iceberg, Java, and Python versions compatible on Nodes 1-3.
