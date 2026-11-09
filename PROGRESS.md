# Distributed Search Engine Progress

## Environment

- Python: `/usr/bin/python3.10` (3.10.22).
- Hadoop: 3.3.6 at `/opt/hadoop`; Spark 3.3.4 / Scala 2.12 is available at `/opt/spark/bin/spark-submit`.
- The project `.venv` contains PySpark 4.2.1 and Node 4 dependencies; the installed
  Spark launcher/cluster is 3.3.4, so local verification used `/opt/spark/bin/spark-submit`
  with the matching Iceberg 3.3 runtime package.
- The repository `.config` is populated for local mode (`HDFS_REPLICATION=1`, no second DataNode).
- Existing `.config` keys can be overridden by environment variables for local test runs;
  no new configuration keys are introduced.

## Checklist

| Area | Status |
|---|---|
| `config_loader.py` and `.config` | done+tested (compile and config smoke test) |
| Cluster lifecycle scripts | done+tested (bash syntax) |
| Iceberg table/admin scripts | done+tested (compile) |
| Ingestion and upload scripts | done+tested (compile and bash syntax) |
| Index, PageRank, export and pipeline | done+tested (compile and bash syntax) |
| Search service, pull script and frontend | done+tested (compile and bash syntax) |
| End-to-end Phase 0/1 run | done+tested (local HDFS, local Spark, FastAPI) |

## Decisions and deviations

- `.config` was empty, so it was created from the README specification; no new configuration keys were added.
- Phase 0 contains 1,040 rows: 20 malformed, 20 duplicate records, and 1,000 accepted.
- Phase 1 contains 100 valid records: 50 deletes and 50 upserts; it produced 975 docs.
- Local Phase 0/1 indexing, PageRank, export, hot reload, rollback, time travel,
  failed-reload preservation, corrupt-export rejection, and idempotent re-ingestion passed.
- Phase 1 re-ingestion now produces no content or snapshot change.
- The Iceberg coordinate is set to the Spark 3.3/Scala 2.12 runtime (`1.5.2`).

## Open items

- Align the root `.venv` PySpark version with the cluster Spark 3.3.4 before using
  the plain `python` wrappers; direct local verification used the matching Spark launcher.
- Configure `/etc/hosts`, Hadoop XML files, and the NameNode once before a 4-VM run.
