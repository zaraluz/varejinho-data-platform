# Databricks notebook source
# validation/facts/verify_fact_maturity_fixture.py
# Gate D6A — verifica que partição aberta ficou fora da Silver.

from datetime import date
from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = f"{CATALOG}.control._d6a_pedido_bronze"
SILVER = f"{CATALOG}.silver._d6a_pedido"
CONTROL = f"{CATALOG}.control._d6a_fact_watermark"
QUAR = f"{CATALOG}.silver._d6a_quarantine_pedido"
HIST = f"{CATALOG}.silver._d6a_quarantine_history_pedido"

checks = []


def check(name, condition, detail=""):
    ok = bool(condition)
    checks.append((name, ok, detail))
    print(f"{'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))


df = spark.table(SILVER)

check("cardinalidade final = 3", df.count() == 3, f"rows={df.count()}")
check(
    "910001 histórico preservado",
    df.filter(F.col("id") == "910001").count() == 1,
)
check(
    "910002 ficou no estado maduro D2, não no estado aberto D3",
    df.filter(
        (F.col("id") == "910002")
        & (F.col("id_situacaopedido") == "2")
        & (F.col("ingestion_date") == F.lit("2026-09-02").cast("date"))
    ).count() == 1,
)
check(
    "910003 foi inserido por D2",
    df.filter(
        (F.col("id") == "910003")
        & (F.col("ingestion_date") == F.lit("2026-09-02").cast("date"))
    ).count() == 1,
)
check(
    "910004 da partição aberta D3 não entrou",
    df.filter(F.col("id") == "910004").count() == 0,
)
check(
    "nenhuma linha D3 entrou na Silver",
    df.filter(F.col("ingestion_date") > F.lit("2026-09-02").cast("date")).count() == 0,
)

state = spark.table(CONTROL).filter(F.col("entity") == "pedido").collect()[0]
check(
    "watermark committed em D2",
    state["last_processed_snapshot"] == date(2026, 9, 2),
    f"committed={state['last_processed_snapshot']}",
)
check(
    "candidate limpo e status COMMITTED",
    state["candidate_snapshot"] is None and state["status"] == "COMMITTED",
    f"candidate={state['candidate_snapshot']} status={state['status']}",
)

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)

print(f"\n=== RESULTADO D6A: {passed}/{total} checks passaram ===")

if passed != total:
    raise Exception("Gate D6A falhou; sandbox preservado para diagnóstico.")

for table in [HIST, QUAR, SILVER, CONTROL, BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("✅ Mature cutoff comprovado: partição aberta não foi processada.")
print("✅ Sandbox removido após sucesso.")
