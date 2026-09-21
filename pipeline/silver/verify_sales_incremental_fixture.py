# Databricks notebook source
# pipeline/silver/verify_sales_incremental_fixture.py
# Gate D7C — verificação final + replay no-op.

from datetime import date
from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = f"{CATALOG}.control._d7c_venda_bronze"
SILVER = f"{CATALOG}.silver._d7c_venda"
CONTROL = f"{CATALOG}.control._d7c_fact_watermark"
QUAR = f"{CATALOG}.silver._d7c_quarantine_venda"
HIST = f"{CATALOG}.silver._d7c_quarantine_history_venda"

checks = []


def check(name, condition, detail=""):
    ok = bool(condition)
    checks.append((name, ok, detail))
    print(f"{'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))


df = spark.table(SILVER)
rows = df.count()

check("cardinalidade final = 3", rows == 3, f"rows={rows}")
check(
    "920001 histórico preservado",
    df.filter(F.col("id") == "920001").count() == 1,
)
check(
    "920002 avançou somente até D2 madura",
    df.filter(
        (F.col("id") == "920002")
        & (F.col("ingestion_date") == F.lit("2026-09-02").cast("date"))
    ).count() == 1,
)
check(
    "920003 foi inserido em D2",
    df.filter(
        (F.col("id") == "920003")
        & (F.col("ingestion_date") == F.lit("2026-09-02").cast("date"))
    ).count() == 1,
)
check(
    "920004 da partição aberta D3 não entrou",
    df.filter(F.col("id") == "920004").count() == 0,
)
check(
    "920005 inválido não entrou",
    df.filter(F.col("id") == "920005").count() == 0,
)
check(
    "nenhuma linha acima de D2",
    df.filter(F.col("ingestion_date") > F.lit("2026-09-02").cast("date")).count() == 0,
)

q_count = spark.table(QUAR).count() if spark.catalog.tableExists(QUAR) else 0
h_count = spark.table(HIST).count() if spark.catalog.tableExists(HIST) else 0
check("quarentena atual = 1", q_count == 1, f"rows={q_count}")
check("quarentena histórica = 1", h_count == 1, f"rows={h_count}")

state = (
    spark.table(CONTROL)
    .filter(F.col("entity") == "venda")
    .collect()[0]
)

check(
    "watermark committed em D2",
    state["last_processed_snapshot"] == date(2026, 9, 2),
    f"committed={state['last_processed_snapshot']}",
)
check(
    "candidate limpo e COMMITTED",
    state["candidate_snapshot"] is None and state["status"] == "COMMITTED",
    f"candidate={state['candidate_snapshot']} status={state['status']}",
)

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)

print(f"\n=== RESULTADO D7C: {passed}/{total} checks passaram ===")

if passed != total:
    raise Exception("Gate D7C falhou; sandbox preservado para diagnóstico.")

for table in [HIST, QUAR, SILVER, CONTROL, BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("✅ Venda incremental D+1 aprovada.")
print("✅ Reexecução após commit foi no-op e D3 aberta permaneceu fora.")
print("✅ Sandbox removido após sucesso.")
