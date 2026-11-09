#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }

mkdir -p "$(abs Cluster_1/work)"
if ! hdfs getconf -confKey dfs.namenode.name.dir >/dev/null 2>&1; then
  echo "Hadoop is not configured; set HADOOP_CONF_DIR first" >&2
  exit 1
fi
hdfs --daemon start namenode
hdfs --daemon start datanode
for _ in $(seq 1 30); do
  if hdfs dfsadmin -safemode get >/dev/null 2>&1; then break; fi
  sleep 2
done
for d in RAW_DIR WAREHOUSE INDEX_DIR EXPORT_DIR REJECT_DIR CHECKPOINT_DIR; do
  var="HDFS_$d"
  hdfs dfs -mkdir -p "$HDFS_URI${!var}"
done
export SPARK_WORKER_DIR="$(abs Cluster_1/work)"
"$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"
echo "Node 2 started"
