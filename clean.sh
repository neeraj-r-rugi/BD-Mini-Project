#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a
source "$ROOT/.config"
set +a

abs() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$ROOT" "$1" ;;
  esac
}

HDFS_ROOT="${HDFS_URI%/}${HDFS_BASE}"
REPORTS="$(abs "$REPORT_DIR")"
STAGING="$(abs "$STAGING_DIR")"
SERVING="$(abs "$SERVING_DIR")"
CLUSTER1_WORK="$(abs "Cluster_1/work")"
CLUSTER2_WORK="$(abs "Cluster_2/work")"

echo "Cleaning generated pipeline state..."
echo "  HDFS: $HDFS_ROOT"
echo "  Local reports: $REPORTS"
echo "  Local staging: $STAGING"
echo "  Local serving: $SERVING"

if ! command -v hdfs >/dev/null 2>&1; then
  echo "error: hdfs command not found; HDFS state was not removed" >&2
  exit 1
fi

hdfs dfs -rm -r -f "$HDFS_ROOT"

rm -rf \
  "$REPORTS" \
  "$STAGING" \
  "$SERVING" \
  "$CLUSTER1_WORK" \
  "$CLUSTER2_WORK" \
  "$ROOT/__pycache__" \
  "$ROOT/Cluster_1/__pycache__" \
  "$ROOT/Cluster_2/__pycache__" \
  "$ROOT/Ingestion_Node/__pycache__" \
  "$ROOT/Search_Node/__pycache__"

mkdir -p "$REPORTS" "$STAGING" "$SERVING"

echo "Clean complete."
echo "Preserved: .config, source files, documentation, .venv, and Ingestion_Node/data/*.jsonl"
echo "Restart the search service before using it again if it was running during cleanup."
