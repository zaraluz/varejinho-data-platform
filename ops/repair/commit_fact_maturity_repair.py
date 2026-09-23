# Databricks notebook source
# ops/repair/commit_fact_maturity_repair.py
# Gate D6B — commit especial que permite corrigir watermark para trás se necessário.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")

if not CATALOG.endswith("_dev") or not ENTITY:
    raise Exception(f"D6B commit inválido: catalog={CATALOG} entity={ENTITY}")

rows = spark.table(CONTROL_TABLE).filter(F.col("entity") == ENTITY).collect()
if len(rows) != 1:
    raise Exception(f"{ENTITY}: watermark esperado=1; encontrado={len(rows)}")

row = rows[0]
committed_before = row["last_processed_snapshot"]
candidate = row["candidate_snapshot"]
status = row["status"]

if status != "REPAIR_PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"{ENTITY}: estado inválido para repair commit: "
        f"committed={committed_before} candidate={candidate} status={status}"
    )

spark.sql(f"""
    UPDATE {CONTROL_TABLE}
    SET last_processed_snapshot = candidate_snapshot,
        candidate_snapshot = NULL,
        status = 'COMMITTED',
        updated_at = current_timestamp()
    WHERE entity = '{ENTITY}'
      AND status = 'REPAIR_PENDING_VALIDATION'
""")

final = spark.table(CONTROL_TABLE).filter(F.col("entity") == ENTITY).collect()[0]

if final["status"] != "COMMITTED" or final["candidate_snapshot"] is not None:
    raise Exception(f"{ENTITY}: repair commit não concluiu")

print(
    f"✅ {ENTITY}: watermark corrigido {committed_before} -> "
    f"{final['last_processed_snapshot']} | status=COMMITTED"
)
