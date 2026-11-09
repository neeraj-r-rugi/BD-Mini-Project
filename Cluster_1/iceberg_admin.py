"""Node 2 - Iceberg maintenance CLI."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import CFG, build_spark, table
from pyspark.sql import functions as F


def read_at(spark, snapshot_id=None):
    reader = spark.read.format("iceberg")
    if snapshot_id is not None:
        reader = reader.option("snapshot-id", str(snapshot_id))
    return reader.load(table())


def cmd_snapshots(spark, _args):
    spark.sql(
        f"SELECT snapshot_id, committed_at, operation, summary "
        f"FROM {table()}.snapshots ORDER BY committed_at"
    ).show(truncate=False)


def cmd_current_snapshot(spark, _args):
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table()}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if rows:
        print(rows[0]["snapshot_id"])


def cmd_checksum(spark, args):
    df = read_at(spark, args.snapshot)
    checksum = df.select(
        F.sum(F.hash("doc_id", "url", "title", "text", "links", "version")).alias(
            "checksum"
        ),
        F.count("*").alias("count"),
    ).first()
    print(f"count={checksum['count']} checksum={checksum['checksum'] or 0}")


def cmd_timetravel(spark, args):
    df = read_at(spark, args.id)
    print(f"count={df.count()}")
    df.select("doc_id", "title").limit(5).show(truncate=False)


def cmd_rollback(spark, args):
    catalog = CFG["ICEBERG_CATALOG"]
    target = f"{CFG['ICEBERG_NAMESPACE']}.{CFG['ICEBERG_DOCS_TABLE']}"
    spark.sql(
        f"CALL {catalog}.system.rollback_to_snapshot('{target}', {args.id})"
    )
    cmd_current_snapshot(spark, args)


def build_parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("snapshots")
    sub.add_parser("current-snapshot")
    checksum = sub.add_parser("checksum")
    checksum.add_argument("--snapshot")
    travel = sub.add_parser("timetravel")
    travel.add_argument("id")
    rollback = sub.add_parser("rollback")
    rollback.add_argument("id")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    spark = build_spark("iceberg-admin", node=2)
    try:
        globals()[f"cmd_{args.command.replace('-', '_')}"](spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
