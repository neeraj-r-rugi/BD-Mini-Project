#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }

PHASE="${1:?usage: upload_to_hdfs.sh <phase>}"
FILE_VAR="PHASE_${PHASE}_FILE"
FILE="${!FILE_VAR:?missing configuration: $FILE_VAR}"
LOCAL="$(abs "$LOCAL_DATA_DIR")/$FILE"
DEST="$HDFS_URI$HDFS_RAW_DIR/$FILE"
[[ -f "$LOCAL" ]] || { echo "missing input: $LOCAL" >&2; exit 1; }
hdfs dfs -mkdir -p "$HDFS_URI$HDFS_RAW_DIR"
hdfs dfs -D dfs.replication="$HDFS_REPLICATION" -put -f "$LOCAL" "$DEST"
echo "$DEST"
