# Databricks notebook source
# validation/release/fixture_partition_manifest.py
# R2 fixture — proves post-commit partition mutation detection in isolation.

import importlib.util
from datetime import date

from pyspark.sql import functions as F


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


def required_param(nome: str) -> str:
    """Parâmetro obrigatório do job: falha cedo em vez de cair num default de ambiente."""
    try:
        value = dbutils.widgets.get(nome)
    except Exception:
        value = ""
    if not value:
        raise ValueError(
            f"Parâmetro obrigatório ausente: '{nome}'. Execute via job do bundle, "
            "que injeta catalog/bundle_files_path/control_root/bronze_source_catalog por target."
        )
    return value


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param(
    "control_root",
    "s3://varejinho-lake/_control/dev/r2_partition_manifest_fixture",
).rstrip("/")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")

if not CATALOG.endswith("_dev"):
    raise Exception(f"R2 fixture is dev-only. Received: {CATALOG}")

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest.py"
spec = importlib.util.spec_from_file_location("partition_manifest_fixture", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load partition manifest engine: {ENGINE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

FactPartitionManifestGuard = module.FactPartitionManifestGuard
PartitionManifestViolation = module.PartitionManifestViolation

GUARD = FactPartitionManifestGuard(
    spark=spark,
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
)

DATA_PATH = f"{CONTROL_ROOT}/source"
SOURCE_TABLE = f"{CATALOG}.control._r2_partition_manifest_source"

dbutils.fs.rm(CONTROL_ROOT, True)
spark.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.control")


def append_snapshot(day: str, rows):
    df = spark.createDataFrame(
        [(str(id_), value, date.fromisoformat(day)) for id_, value in rows],
        ["id", "value", "ingestion_date"],
    )
    df.coalesce(1).write.format("parquet").mode("append").save(DATA_PATH)


def register_source():
    spark.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    spark.sql(
        f"""
        CREATE TABLE {SOURCE_TABLE}
        USING PARQUET
        LOCATION '{DATA_PATH}'
        """
    )


checks = []


def check(name: str, ok: bool, detail: str = ""):
    checks.append((name, ok, detail))
    print(f"{'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))


# D1 = committed baseline.
append_snapshot("2026-09-01", [(1, "A"), (2, "B")])
register_source()

boot = GUARD.bootstrap(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 1),
)
check(
    "BOOTSTRAP D1",
    boot["created"] and boot["rows"] == 1,
    f"created={boot['created']} rows={boot['rows']}",
)

stable = GUARD.assert_committed_unchanged(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 1),
)
check(
    "STABLE COMMITTED D1",
    stable["ok"],
    f"partitions={stable['manifest_rows']}",
)

# D2 is validated, rechecked at commit, then promoted.
append_snapshot("2026-09-02", [(1, "A2"), (3, "C")])
register_source()
stage = GUARD.stage_validated(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 1),
    date(2026, 9, 2),
)
promote = GUARD.promote_validated(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 1),
    date(2026, 9, 2),
)
check(
    "D2 VALIDATE -> COMMIT",
    stage["staged_rows"] == 1 and promote["promoted_rows"] == 1,
    f"staged={stage['staged_rows']} promoted={promote['promoted_rows']}",
)

# Simula crash boundary: manifest já foi promovido, mas o watermark ainda
# apontaria para D1. O retry do COMMIT deve reconhecer D2 como já promovido.
promote_retry = GUARD.promote_validated(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 1),
    date(2026, 9, 2),
)
check(
    "COMMIT RETRY AFTER MANIFEST PROMOTION",
    promote_retry["reused"] and promote_retry["promoted_rows"] == 1,
    f"reused={promote_retry['reused']} rows={promote_retry['promoted_rows']}",
)

replay = GUARD.assert_committed_unchanged(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 2),
)
check(
    "REPLAY AFTER D2 COMMIT",
    replay["ok"],
    f"partitions={replay['manifest_rows']}",
)

# D3 changes after VALIDATE but before COMMIT: must block.
append_snapshot("2026-09-03", [(4, "D")])
register_source()
GUARD.stage_validated(
    "fixture_fact",
    SOURCE_TABLE,
    date(2026, 9, 2),
    date(2026, 9, 3),
)

append_snapshot("2026-09-03", [(5, "LATE")])
register_source()

blocked_between = False
try:
    GUARD.promote_validated(
        "fixture_fact",
        SOURCE_TABLE,
        date(2026, 9, 2),
        date(2026, 9, 3),
    )
except PartitionManifestViolation as exc:
    blocked_between = True
    print(f"EXPECTED BLOCK: {exc}")

check(
    "MUTATION BETWEEN VALIDATE AND COMMIT BLOCKS",
    blocked_between,
)

# D1 changes after it was already committed: next guard must block.
append_snapshot("2026-09-01", [(99, "LATE_OLD_PARTITION")])
register_source()

blocked_old = False
try:
    GUARD.assert_committed_unchanged(
        "fixture_fact",
        SOURCE_TABLE,
        date(2026, 9, 2),
    )
except PartitionManifestViolation as exc:
    blocked_old = True
    print(f"EXPECTED BLOCK: {exc}")

check(
    "POST-COMMIT OLD PARTITION MUTATION BLOCKS",
    blocked_old,
)

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)

print("\n=== R2 PARTITION MANIFEST FIXTURE ===")
for name, ok, detail in checks:
    print(f"{'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))
print(f"\n{passed}/{total} checks passed")

spark.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
dbutils.fs.rm(CONTROL_ROOT, True)

if passed != total:
    raise Exception(f"R2 fixture failed: {passed}/{total}")

print("✅ Mutation guard semantics proven in isolated sandbox.")
