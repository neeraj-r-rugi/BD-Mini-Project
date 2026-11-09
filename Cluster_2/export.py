"""Node 3 - validate index outputs and export plain Parquet."""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import build_spark, CFG, hdfs, table
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--version", type=int, required=True)
    return parser.parse_args()


def load_inputs(spark, snapshot_id):
    base = hdfs("HDFS_INDEX_DIR")
    postings = spark.read.parquet(f"{base}/postings")
    stats = spark.read.parquet(f"{base}/doc_stats")
    ranks = spark.read.parquet(f"{base}/pagerank")
    meta = spark.read.option("snapshot-id", snapshot_id).format("iceberg").load(
        table()
    ).select("doc_id", "url", "title")
    return postings, stats, ranks, meta


def build_docs_export(docs_meta, doc_stats, pagerank):
    return docs_meta.join(doc_stats, "doc_id").join(pagerank, "doc_id")


def validate(postings, docs_export, pagerank):
    n_docs = docs_export.count()
    if n_docs == 0:
        raise ValueError("export contains no documents")
    if pagerank.count() != n_docs:
        raise ValueError("PageRank/document counts differ")
    total = pagerank.agg(F.sum("pagerank")).first()[0]
    if abs(total - 1.0) >= 1e-6:
        raise ValueError(f"PageRank sum is {total}")
    if postings.join(docs_export.select("doc_id"), "doc_id", "left_anti").limit(1).count():
        raise ValueError("postings contain an unknown document")
    if docs_export.filter(
        F.col("url").isNull() | (F.trim("url") == "") |
        F.col("title").isNull() | (F.trim("title") == "")
    ).limit(1).count():
        raise ValueError("export contains a blank URL or title")


def write_export(spark, version, snapshot_id, postings, docs_export):
    base = f"{hdfs('HDFS_EXPORT_DIR')}/v{version}.tmp"
    postings.coalesce(1).write.mode("overwrite").parquet(f"{base}/postings")
    docs_export.select("doc_id", "url", "title", "doc_len", "pagerank").coalesce(
        1
    ).write.mode("overwrite").parquet(f"{base}/docs")
    manifest = spark.createDataFrame(
        [[
            version,
            str(snapshot_id),
            docs_export.count(),
            postings.count(),
            datetime.now(timezone.utc).isoformat(),
        ]],
        "version int, snapshot_id string, n_docs long, n_postings long, created_at string",
    )
    manifest.coalesce(1).write.mode("overwrite").parquet(f"{base}/manifest")


def main():
    args = parse_args()
    spark = build_spark("export", node=3)
    try:
        postings, stats, ranks, meta = load_inputs(spark, args.snapshot)
        docs = build_docs_export(meta, stats, ranks)
        validate(postings, docs, ranks)
        write_export(spark, args.version, args.snapshot, postings, docs)
        print(f"EXPORT OK v{args.version} docs={docs.count()} postings={postings.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
