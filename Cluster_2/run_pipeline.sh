#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }

VERSION="${1:?usage: run_pipeline.sh <version>}"

# Read the current snapshot id. Spark/Ivy can print log lines to stdout, so
# capture everything, then keep only the last purely numeric line.
SNAP_OUT="$(python "$ROOT/Cluster_1/iceberg_admin.py" current-snapshot)"
SNAP="$(printf '%s\n' "$SNAP_OUT" | grep -E '^[0-9]+$' | tail -1 || true)"
if ! [[ "$SNAP" =~ ^[0-9]+$ ]]; then
  echo "no current Iceberg snapshot (could not parse a numeric id)" >&2
  echo "--- last lines of iceberg_admin.py output ---" >&2
  printf '%s\n' "$SNAP_OUT" | tail -20 >&2
  exit 1
fi
echo "Using snapshot: $SNAP"

start=$(date +%s); python "$ROOT/Cluster_2/build_index.py" --snapshot "$SNAP"; end=$(date +%s)
echo "index: $((end-start))s"
start=$(date +%s); python "$ROOT/Cluster_2/pagerank.py" --snapshot "$SNAP"; end=$(date +%s)
echo "pagerank: $((end-start))s"
start=$(date +%s); python "$ROOT/Cluster_2/export.py" --snapshot "$SNAP" --version "$VERSION"; end=$(date +%s)
echo "export: $((end-start))s"
E="$HDFS_URI$HDFS_EXPORT_DIR"
hdfs dfs -rm -r -f "$E/v$VERSION"
hdfs dfs -mv "$E/v$VERSION.tmp" "$E/v$VERSION"
S="$(abs "$STAGING_DIR")"
mkdir -p "$S"
rm -rf "$S/v$VERSION"
hdfs dfs -get "$E/v$VERSION" "$S/v$VERSION"
echo "READY: $S/v$VERSION"