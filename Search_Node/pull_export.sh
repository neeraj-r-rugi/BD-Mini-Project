#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; source "$ROOT/.config"; set +a
abs() { case "$1" in /*) echo "$1";; *) echo "$ROOT/$1";; esac; }

VERSION="${1:?usage: pull_export.sh <version>}"
SERVING="$(abs "$SERVING_DIR")"
TMP="$SERVING/v$VERSION.tmp"
mkdir -p "$SERVING"
rm -rf "$TMP"
resolved="$(getent hosts "$NODE3_HOST" | awk '{print $1}' | head -1 || true)"
if [[ "$SPARK_MASTER_URL" == local* ||
      "$HDFS_URI" == hdfs://localhost:* ||
      "$HDFS_URI" == hdfs://127.*:* ||
      -z "$resolved" ||
      "$resolved" =~ ^(127\.|::1$) ]]; then
  mkdir -p "$TMP"
  cp -r "$(abs "$STAGING_DIR")/v$VERSION/." "$TMP/"
else
  rsync -az --delete "$SSH_USER@$NODE3_HOST:$ROOT/Cluster_2/export_staging/v$VERSION/" "$TMP/"
fi
for part in postings docs manifest; do
  [[ -f "$TMP/$part/_SUCCESS" ]] || {
    rm -rf "$TMP"
    echo "incomplete export: $part" >&2
    exit 1
  }
done
rm -rf "$SERVING/v$VERSION"
mv "$TMP" "$SERVING/v$VERSION"
CODE="$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://localhost:$SEARCH_PORT/reload?version=$VERSION" || true)"
case "$CODE" in
  200) ln -sfn "v$VERSION" "$SERVING/current"; echo "switched to v$VERSION" ;;
  000) ln -sfn "v$VERSION" "$SERVING/current"; echo "service not running; start it to serve v$VERSION" ;;
  *) mv "$SERVING/v$VERSION" "$SERVING/v$VERSION.rejected"; echo "REJECTED (HTTP $CODE)" >&2; exit 1 ;;
esac
