"""Node 3 - PageRank over a pinned snapshot."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import CFG, build_spark, hdfs, table
from pyspark.sql import functions as F

DAMPING = float(CFG["PAGERANK_DAMPING"])
ITERATIONS = int(CFG["PAGERANK_ITERATIONS"])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    return parser.parse_args()


def load_graph(spark, snapshot_id):
    docs = spark.read.option("snapshot-id", snapshot_id).format("iceberg").load(
        table()
    ).select("doc_id", "links").cache()
    nodes = docs.select(F.col("doc_id").alias("id"))
    edges = docs.select(
        F.col("doc_id").alias("src"), F.explode("links").alias("dst")
    ).join(nodes.select(F.col("id").alias("dst")), "dst")
    outdeg = edges.groupBy("src").agg(F.count("*").alias("deg"))
    links = edges.join(outdeg, "src").cache()
    sinks = nodes.join(outdeg, nodes.id == outdeg.src, "left_anti").select("id")
    return nodes, links, sinks, nodes.count()


def iterate(nodes, links, sinks, ranks, n):
    contributions = (
        links.join(ranks, links.src == ranks.id)
        .select(F.col("dst").alias("id"), (F.col("rank") / F.col("deg")).alias("c"))
        .groupBy("id")
        .agg(F.sum("c").alias("contrib"))
    )
    sink_mass = (
        ranks.join(sinks, "id").agg(F.sum("rank").alias("mass")).first()["mass"] or 0.0
    )
    return (
        nodes.join(contributions, "id", "left")
        .select(
            "id",
            (
                F.lit((1 - DAMPING) / n)
                + F.lit(DAMPING)
                * (
                    F.coalesce("contrib", F.lit(0.0))
                    + F.lit(sink_mass / n)
                )
            ).alias("rank"),
        )
        .checkpoint()
    )


def main():
    args = parse_args()
    spark = build_spark("pagerank", node=3)
    try:
        nodes, links, sinks, n = load_graph(spark, args.snapshot)
        if n == 0:
            raise ValueError("cannot calculate PageRank for an empty table")
        ranks = nodes.withColumn("rank", F.lit(1.0 / n))
        for iteration in range(ITERATIONS):
            ranks = iterate(nodes, links, sinks, ranks, n)
            print(f"iteration={iteration + 1} sum={ranks.agg(F.sum('rank')).first()[0]}", file=sys.stderr)
        total = ranks.agg(F.sum("rank")).first()[0]
        if abs(total - 1.0) >= 1e-6:
            raise ValueError(f"PageRank sum is {total}, expected 1")
        ranks.select(F.col("id").alias("doc_id"), "rank").withColumnRenamed(
            "rank", "pagerank"
        ).write.mode("overwrite").parquet(f"{hdfs('HDFS_INDEX_DIR')}/pagerank")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
