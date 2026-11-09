#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
"$SPARK_HOME/sbin/stop-worker.sh" || true
"$SPARK_HOME/sbin/stop-master.sh" || true
if [[ "$CLUSTER2_RUN_DATANODE" == true ]]; then
  hdfs --daemon stop datanode || true
fi
