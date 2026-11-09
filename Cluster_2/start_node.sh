#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }

"$SPARK_HOME/sbin/start-master.sh"
export SPARK_WORKER_DIR="$(abs Cluster_2/work)"
mkdir -p "$SPARK_WORKER_DIR"
"$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"
if [[ "$CLUSTER2_RUN_DATANODE" == true ]]; then
  hdfs --daemon start datanode
fi
echo "Spark master: http://$NODE3_HOST:8080"
