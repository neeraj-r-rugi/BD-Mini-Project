"""Node 3 - build a distributed inverted index from a pinned snapshot."""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import CFG, build_spark, hdfs, table
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    return parser.parse_args()


def read_docs(spark, snapshot_id):
    return spark.read.option("snapshot-id", snapshot_id).format("iceberg").load(
        table()
    ).select("doc_id", "title", "text")


def tokenize(docs):
    regex = CFG["TOKEN_REGEX"].replace("'", "\\'")
    expression = (
        f"regexp_extract_all(lower(concat_ws(' ', title, text)), '{regex}', 0)"
    )
    return docs.select("doc_id", F.expr(expression).alias("tokens"))


def build_postings(tokens_df):
    return (
        tokens_df.select("doc_id", F.explode("tokens").alias("term"))
        .groupBy("term", "doc_id")
        .agg(F.count("*").alias("tf"))
    )


def build_doc_stats(tokens_df):
    return tokens_df.select("doc_id", F.size("tokens").alias("doc_len"))


def main():
    args = parse_args()
    spark = build_spark("build-index", node=3)
    try:
        tokens = tokenize(read_docs(spark, args.snapshot)).cache()
        postings = build_postings(tokens)
        stats = build_doc_stats(tokens)
        base = hdfs("HDFS_INDEX_DIR")
        postings.write.mode("overwrite").parquet(f"{base}/postings")
        stats.write.mode("overwrite").parquet(f"{base}/doc_stats")
        print(
            f"docs={stats.count()} terms={postings.select('term').distinct().count()} "
            f"postings={postings.count()}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
