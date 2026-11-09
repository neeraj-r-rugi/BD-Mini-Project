#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
"$SPARK_HOME/sbin/stop-worker.sh" || true
hdfs --daemon stop datanode || true
hdfs --daemon stop namenode || true
