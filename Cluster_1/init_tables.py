"""Node 2 - create the Iceberg namespace and docs table."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import CFG, build_spark, table


def main() -> None:
    spark = build_spark("init-tables", node=2)
    try:
        catalog = CFG["ICEBERG_CATALOG"]
        namespace = CFG["ICEBERG_NAMESPACE"]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {table()} (
            doc_id string, url string, title string, text string,
            links array<string>, version bigint) USING iceberg"""
        )
        spark.sql(f"DESCRIBE {table()}").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
