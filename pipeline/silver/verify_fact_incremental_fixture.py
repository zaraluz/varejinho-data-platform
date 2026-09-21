# Databricks notebook source
# pipeline/silver/verify_fact_incremental_fixture.py
# Gate D4 — verifica semântica final do runtime incremental em sandbox.

from datetime import date
from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
SILVER = f"{CATALOG}.silver._d4_pedido"
CONTROL = f"{CATALOG}.control._d4_fact_watermark"
QUAR = f"{CATALOG}.silver._d4_quarantine_pedido"
HIST = f"{CATALOG}.silver._d4_quarantine_history_pedido"
BRONZE = f"{CATALOG}.control._d4_pedido_bronze"

checks = []


def check(nome, cond, detail=""):
    ok = bool(cond)
    checks.append((nome, ok, detail))
    print(f"{'✅' if ok else '❌'} {nome}" + (f" — {detail}" if detail else ""))


df = spark.table(SILVER)
rows = df.count()
check("cardinalidade final = 3", rows == 3, f"rows={rows}")

check(
    "900001 preservado apesar de ausente em D2/D3",
    df.filter(F.col("id") == "900001").count() == 1,
)

check(
    "900002 recebeu último update de D3",
    df.filter(
        (F.col("id") == "900002")
        & (F.col("id_situacaopedido") == "3")
        & (F.col("ingestion_date") == F.lit("2026-09-03").cast("date"))
    ).count() == 1,
)

check(
    "900003 foi inserido e está no último estado",
    df.filter(
        (F.col("id") == "900003")
        & (F.col("ingestion_date") == F.lit("2026-09-03").cast("date"))
    ).count() == 1,
)

check(
    "900004 inválido não entrou na Silver",
    df.filter(F.col("id") == "900004").count() == 0,
)

q_count = spark.table(QUAR).count() if spark.catalog.tableExists(QUAR) else 0
h_count = spark.table(HIST).count() if spark.catalog.tableExists(HIST) else 0
check("quarentena atual recebeu 1 inválido", q_count == 1, f"rows={q_count}")
check("histórico de quarentena recebeu 1 inválido", h_count == 1, f"rows={h_count}")

state = spark.table(CONTROL).filter(F.col("entity") == "pedido").collect()[0]
check(
    "watermark final = D3",
    state["last_processed_snapshot"] == date(2026, 9, 3),
    f"committed={state['last_processed_snapshot']}",
)
check(
    "candidate limpo e status COMMITTED",
    state["candidate_snapshot"] is None and state["status"] == "COMMITTED",
    f"candidate={state['candidate_snapshot']} status={state['status']}",
)

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)
print(f"\n=== RESULTADO D4: {passed}/{total} checks passaram ===")

if passed != total:
    raise Exception("Gate D4 falhou; sandbox preservado para diagnóstico.")

for table in [HIST, QUAR, SILVER, CONTROL, BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("✅ Runtime incremental sandbox aprovado.")
print("✅ Update + insert + no-delete + quarantine + watermark comprovados.")
print("✅ Tabelas sandbox removidas após sucesso.")
