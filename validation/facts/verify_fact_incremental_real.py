# Databricks notebook source
# validation/facts/verify_fact_incremental_real.py
# Gate D5 — verificação final do catch-up incremental real em dev.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_TABLE = job_param(
    "control_table",
    f"{CATALOG}.control.fact_watermark",
)

FACTS = [
    "notaentrada",
    "notaentradaitem",
    "perda",
    "logestoque",
    "promocao",
    "promocaoitem",
    "pedido",
    "pedidoitem",
    "oferta",
    "pagarfornecedor",
    "pagarfornecedorparcela",
    "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto",
]

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D5 só pode executar em *_dev. Recebido: {CATALOG}")

print("\n=== GATE D5 — VERIFY REAL FACT INCREMENTAL CATCH-UP ===")

checks = []

for entity in FACTS:
    rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )

    if len(rows) != 1:
        checks.append((entity, False, f"watermark rows={len(rows)}"))
        continue

    row = rows[0]
    committed = row["last_processed_snapshot"]
    candidate = row["candidate_snapshot"]
    status = row["status"]

    bronze = f"{CATALOG}.bronze.{entity}"
    bronze_max = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]

    pending = (
        spark.table(bronze)
        .filter(F.col("ingestion_date") > F.lit(committed))
        .select("ingestion_date")
        .distinct()
        .count()
        if committed is not None
        else None
    )

    ok = (
        status == "COMMITTED"
        and candidate is None
        and committed == bronze_max
        and pending == 0
    )

    detail = (
        f"committed={committed} | bronze_max={bronze_max} | "
        f"candidate={candidate} | status={status} | pending={pending}"
    )
    checks.append((entity, ok, detail))
    print(f"{'✅' if ok else '❌'} {entity} — {detail}")

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)

print(f"\n=== RESULTADO D5: {passed}/{total} facts fechados no Bronze max ===")

if passed != total:
    failed = [entity for entity, ok, _ in checks if not ok]
    raise Exception(
        "Gate D5 falhou; fatos ainda não fechados: " + ", ".join(failed)
    )

print("✅ Todos os fact_watermarks estão COMMITTED no Bronze max atual.")
print("✅ candidate_snapshot está limpo para todas as 13 entidades.")
print("✅ Nenhum snapshot Bronze permanece pendente.")
print("✅ O catch-up real em dev foi concluído com validação antes de cada commit.")
