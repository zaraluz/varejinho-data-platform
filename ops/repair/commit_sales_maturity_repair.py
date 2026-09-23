# Databricks notebook source
# ops/repair/commit_sales_maturity_repair.py
# Gate D7B — promove o mature cutoff de venda após validação.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL = f"{CATALOG}.control.fact_watermark"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D7B só pode executar em *_dev. Recebido: {CATALOG}")

rows = spark.table(CONTROL).filter(F.col("entity") == "venda").collect()
if len(rows) != 1:
    raise Exception(f"venda: watermark esperado=1; encontrado={len(rows)}")

row = rows[0]
candidate = row["candidate_snapshot"]
status = row["status"]

if status != "REPAIR_PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"venda: estado inválido para commit: candidate={candidate} status={status}"
    )

spark.sql(f"""
    UPDATE {CONTROL}
    SET last_processed_snapshot = candidate_snapshot,
        candidate_snapshot = NULL,
        status = 'COMMITTED',
        updated_at = current_timestamp()
    WHERE entity = 'venda'
      AND status = 'REPAIR_PENDING_VALIDATION'
""")

final = spark.table(CONTROL).filter(F.col("entity") == "venda").collect()[0]

if final["status"] != "COMMITTED" or final["candidate_snapshot"] is not None:
    raise Exception("venda: falha ao promover mature cutoff")

print(
    f"✅ venda: committed={final['last_processed_snapshot']} | "
    "candidate=None | status=COMMITTED"
)
