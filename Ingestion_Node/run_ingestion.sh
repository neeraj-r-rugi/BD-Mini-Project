#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }

PHASE="${1:?usage: run_ingestion.sh <phase>}"
"$ROOT/Ingestion_Node/upload_to_hdfs.sh" "$PHASE"
python "$ROOT/Ingestion_Node/ingest.py" --phase "$PHASE"
echo "report: $(abs "$REPORT_DIR")/phase_${PHASE}.json"
