# Databricks notebook source
# validation/scd2/verify_scd2_product_generic_regression.py
# Gate B7H — prova que o engine genérico reproduz exatamente o backfill aprovado de produto.
# Limpa sandbox somente após sucesso.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
TEST_BRONZE = f"{CATALOG}.control._b7h_product_test_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._b7h_product_baseline_bronze"
GENERIC_SILVER = f"{CATALOG}.silver._b7h_product_generic"
EXPECTED_SILVER = f"{CATALOG}.silver._b7h_product_expected"
GENERIC_CONTROL = f"{CATALOG}.control._b7h_product_watermark"
REAL_SILVER = f"{CATALOG}.silver.produto"
REAL_CONTROL = f"{CATALOG}.control.scd2_watermark"
REAL_SILVER_BASELINE = f"{CATALOG}.silver._b7h_product_real_baseline"
REAL_CONTROL_BASELINE = f"{CATALOG}.control._b7h_product_real_control_baseline"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B7H só pode executar em *_dev. Recebido: {CATALOG}")

required = [
    TEST_BRONZE, BASELINE_BRONZE, GENERIC_SILVER, EXPECTED_SILVER,
    GENERIC_CONTROL, REAL_SILVER, REAL_CONTROL,
    REAL_SILVER_BASELINE, REAL_CONTROL_BASELINE,
]
for table in required:
    if not spark.catalog.tableExists(table):
        raise Exception(f"Pré-requisito ausente: {table}")

checks = []


def check(name: str, ok: bool, detail: str = ""):
    prefix = "✅" if ok else "❌"
    msg = f"{prefix} {name}"
    if detail:
        msg += f" — {detail}"
    checks.append((ok, msg))
    print(msg)


generic = spark.table(GENERIC_SILVER)
expected = spark.table(EXPECTED_SILVER)
real = spark.table(REAL_SILVER)
real_before = spark.table(REAL_SILVER_BASELINE)
control = spark.table(REAL_CONTROL)
control_before = spark.table(REAL_CONTROL_BASELINE)
test_bronze = spark.table(TEST_BRONZE)

replay_snapshot = test_bronze.agg(F.max("ingestion_date")).collect()[0][0]

print("\n=== GATE B7H — PRODUCT GENERIC ENGINE REGRESSION ===\n")
print(f"snapshot exercitado: {replay_snapshot}\n")

generic_rows = generic.count()
expected_rows = expected.count()
check(
    "Cardinalidade genérico = backfill esperado",
    generic_rows == expected_rows,
    f"generic={generic_rows:,} | expected={expected_rows:,}",
)

generic_ids = generic.select("id").distinct().count()
expected_ids = expected.select("id").distinct().count()
check(
    "IDs distintos iguais",
    generic_ids == expected_ids,
    f"generic={generic_ids:,} | expected={expected_ids:,}",
)

generic_versioned = generic.groupBy("id").count().filter(F.col("count") > 1).count()
expected_versioned = expected.groupBy("id").count().filter(F.col("count") > 1).count()
check(
    "IDs versionados iguais",
    generic_versioned == expected_versioned,
    f"generic={generic_versioned:,} | expected={expected_versioned:,}",
)

generic_current = generic.filter(F.col("is_current")).count()
expected_current = expected.filter(F.col("is_current")).count()
check(
    "Versões current iguais",
    generic_current == expected_current,
    f"generic={generic_current:,} | expected={expected_current:,}",
)

missing_generic = expected.exceptAll(generic).count()
unexpected_generic = generic.exceptAll(expected).count()
check(
    "Conteúdo exato do engine genérico = backfill determinístico",
    missing_generic == 0 and unexpected_generic == 0,
    f"faltando={missing_generic:,} | inesperadas={unexpected_generic:,}",
)

wm_rows = spark.table(GENERIC_CONTROL).filter(F.col("entity") == "produto").collect()
wm_ok = False
wm_detail = f"linhas={len(wm_rows)}"
if len(wm_rows) == 1:
    row = wm_rows[0]
    wm_ok = (
        row["last_processed_snapshot"] == replay_snapshot
        and row["candidate_snapshot"] is None
        and row["status"] == "COMMITTED"
    )
    wm_detail = (
        f"committed={row['last_processed_snapshot']} | "
        f"candidate={row['candidate_snapshot']} | status={row['status']}"
    )
check("Watermark sandbox terminou no snapshot exercitado", wm_ok, wm_detail)

real_missing = real_before.exceptAll(real).count()
real_unexpected = real.exceptAll(real_before).count()
check(
    "Silver real de produto não foi alterada",
    real_missing == 0 and real_unexpected == 0,
    f"faltando={real_missing:,} | inesperadas={real_unexpected:,}",
)

control_missing = control_before.exceptAll(control).count()
control_unexpected = control.exceptAll(control_before).count()
check(
    "Controle real de watermarks não foi alterado",
    control_missing == 0 and control_unexpected == 0,
    f"faltando={control_missing:,} | inesperadas={control_unexpected:,}",
)

failed = [msg for ok, msg in checks if not ok]
print(f"\n=== RESULTADO B7H: {len(checks)-len(failed)}/{len(checks)} checks passaram ===")

if failed:
    print("❌ Sandbox B7H preservada para investigação.")
    raise Exception("Gate B7H falhou:\n" + "\n".join(failed))

for table in [
    GENERIC_CONTROL, GENERIC_SILVER, EXPECTED_SILVER,
    BASELINE_BRONZE, TEST_BRONZE,
    REAL_SILVER_BASELINE, REAL_CONTROL_BASELINE,
]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("✅ Regressão aprovada: engine genérico reproduziu exatamente o estado SCD2 esperado de produto.")
print("✅ Estado real permaneceu intocado e tabelas sandbox foram removidas.")
