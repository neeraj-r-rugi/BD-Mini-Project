"""Single entry point for project configuration and Spark sessions."""

import re
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _load() -> dict[str, str]:
    """Parse .config with bash-like, previously-defined variable expansion."""
    cfg: dict[str, str] = {}
    for raw in (ROOT / ".config").read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip("'\"")
        value = re.sub(r"\$\{(\w+)\}", lambda m: cfg.get(m.group(1), ""), value)
        cfg[key.strip()] = value
    return cfg


CFG = _load()
# Tests may override existing settings without changing the deployment config.
CFG.update({key: value for key, value in os.environ.items() if key in CFG})


def path(key: str) -> Path:
    """Return a configured path, resolving relative paths from the repository."""
    value = Path(CFG[key])
    return value if value.is_absolute() else ROOT / value


def table(name: str | None = None) -> str:
    """Return the fully-qualified Iceberg table name."""
    return f"{CFG['ICEBERG_CATALOG']}.{CFG['ICEBERG_NAMESPACE']}.{name or CFG['ICEBERG_DOCS_TABLE']}"


def hdfs(key: str) -> str:
    """Return an HDFS URI for a configured HDFS path."""
    return CFG["HDFS_URI"].rstrip("/") + "/" + CFG[key].lstrip("/")


def build_spark(app: str, node: int):
    """Create a configured client-mode Spark session."""
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--packages {CFG['ICEBERG_PACKAGE']} pyspark-shell",
    )
    from pyspark.sql import SparkSession

    catalog = CFG["ICEBERG_CATALOG"]
    spark = (
        SparkSession.builder.appName(app)
        .master(CFG["SPARK_MASTER_URL"])
        .config("spark.driver.host", CFG[f"NODE{node}_HOST"])
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", CFG["SPARK_DRIVER_PORT"])
        .config("spark.blockManager.port", CFG["SPARK_BLOCKMANAGER_PORT"])
        .config("spark.jars.packages", CFG["ICEBERG_PACKAGE"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", hdfs("HDFS_WAREHOUSE"))
        .config("spark.hadoop.dfs.replication", CFG["HDFS_REPLICATION"])
        .config("spark.sql.shuffle.partitions", CFG["SPARK_SHUFFLE_PARTITIONS"])
        .config("spark.executor.memory", CFG["SPARK_EXECUTOR_MEMORY"])
        .config("spark.cores.max", CFG["SPARK_CORES_MAX"])
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(hdfs("HDFS_CHECKPOINT_DIR"))
    spark.sparkContext.setLogLevel("WARN")
    return spark
