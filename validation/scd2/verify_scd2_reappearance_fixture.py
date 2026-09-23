# Databricks notebook source
# validation/scd2/verify_scd2_reappearance_fixture.py
# R3 — compare generic incremental output with independent full backfill.

from pyspark.sql import functions as F


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param(
    "control_root",
    "s3://varejinho-lake/_control/dev/r3_scd2_reappearance",
).rstrip("/")

FULL_BRONZE = f"{CATALOG}.control._r3_supplier_reappearance_full_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._r3_supplier_reappearance_baseline_bronze"
SILVER = f"{CATALOG}.silver._r3_supplier_reappearance_incremental"
EXPECTED = f"{CATALOG}.silver._r3_supplier_reappearance_expected"
CONTROL = f"{CATALOG}.control._r3_supplier_reappearance_watermark"

if not CATALOG.endswith("_dev"):
    raise Exception(f"R3 fixture is dev-only. Received: {CATALOG}")

for table in [FULL_BRONZE, BASELINE_BRONZE, SILVER, EXPECTED, CONTROL]:
    if not spark.catalog.tableExists(table):
        raise Exception(f"Required fixture table missing: {table}")

actual = spark.table(SILVER)
expected = spark.table(EXPECTED)
checks = []


def check(name: str, ok: bool, detail: str = ""):
    checks.append((name, ok, detail))
    print(f"{'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))


schema_actual = [(f.name, f.dataType.simpleString()) for f in actual.schema.fields]
schema_expected = [(f.name, f.dataType.simpleString()) for f in expected.schema.fields]
check("Schema incremental == full backfill", schema_actual == schema_expected)

actual_minus_expected = actual.exceptAll(expected).count()
expected_minus_actual = expected.exceptAll(actual).count()
check(
    "Incremental == full backfill (exceptAll)",
    actual_minus_expected == 0 and expected_minus_actual == 0,
    f"actual-only={actual_minus_expected} expected-only={expected_minus_actual}",
)

test = actual.filter(F.col("id") == "790001").orderBy("valid_from")
rows = test.collect()
check("790001 tem exatamente 2 versões", len(rows) == 2, f"actual={len(rows)}")

if len(rows) == 2:
    check(
        "Gap D2/D3 não cria versão redundante",
        str(rows[0]["valid_from"]) == "2026-01-01 08:00:00"
        and str(rows[0]["valid_to"]) == "2026-09-05 00:00:00",
        f"v1=[{rows[0]['valid_from']}, {rows[0]['valid_to']})",
    )
    check(
        "Reappearance D5 com Type 2 abre nova versão",
        str(rows[1]["valid_from"]) == "2026-09-05 00:00:00"
        and rows[1]["valid_to"] is None
        and rows[1]["is_current"] is True,
        f"v2 valid_from={rows[1]['valid_from']} current={rows[1]['is_current']}",
    )
    check(
        "Type 2 final correto",
        rows[0]["razaosocial"] == "GAP SUPPLIER LTDA"
        and rows[1]["razaosocial"] == "GAP SUPPLIER COMERCIO LTDA",
    )

type1_bad = test.filter(
    (~F.col("nomefantasia").eqNullSafe(F.lit("GAP ONE UPDATED")))
    | (~F.col("id_situacaocadastro").eqNullSafe(F.lit("0")))
    | (~F.col("permitenfsempedido").eqNullSafe(F.lit("Y")))
    | (~F.col("id_tipoempresa").eqNullSafe(F.lit("8")))
).count()
check(
    "Type 1 mudou no reappearance sem criar versão e propagou para todo histórico",
    type1_bad == 0,
    f"mismatches={type1_bad}",
)

d3_versions = test.filter(
    F.col("valid_from") == F.lit("2026-09-03").cast("timestamp")
).count()
check("Nenhuma versão aberta em D3 para SAME_TYPE2", d3_versions == 0)

anchor_versions = actual.filter(F.col("id") == "799999").count()
check("Anchor permanece com 1 versão", anchor_versions == 1, f"actual={anchor_versions}")

current_bad = (
    actual.groupBy("id")
    .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("curr"))
    .filter(F.col("curr") != 1)
    .count()
)
check("Exatamente 1 current por ID", current_bad == 0, f"bad_ids={current_bad}")

wm = spark.table(CONTROL).filter(F.col("entity") == "fornecedor").collect()
wm_ok = (
    len(wm) == 1
    and str(wm[0]["last_processed_snapshot"]) == "2026-09-05"
    and wm[0]["candidate_snapshot"] is None
    and wm[0]["status"] == "COMMITTED"
)
check(
    "Watermark final committed em D5",
    wm_ok,
    (
        f"rows={len(wm)}"
        if len(wm) != 1
        else f"committed={wm[0]['last_processed_snapshot']} "
             f"candidate={wm[0]['candidate_snapshot']} status={wm[0]['status']}"
    ),
)

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)
print(f"\n=== RESULTADO R3 REAPPEARANCE: {passed}/{total} checks passaram ===")

if passed != total:
    print("❌ Sandbox preservada para investigação.")
    raise Exception(f"R3 reappearance fixture failed: {passed}/{total}")

for table in [CONTROL, EXPECTED, SILVER, BASELINE_BRONZE, FULL_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
dbutils.fs.rm(CONTROL_ROOT, True)

print("✅ Reappearance policy proven against independent full backfill.")
print("✅ Synthetic sandbox removed after success.")
