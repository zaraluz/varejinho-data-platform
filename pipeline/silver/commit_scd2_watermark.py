# Databricks notebook source
# pipeline/silver/commit_scd2_watermark.py
# Commit genérico de watermark SCD2 após Quality Gate.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "fornecedor")
BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.{ENTITY}")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.scd2_watermark")

if not CATALOG.endswith("_dev"):
    raise Exception(f"commit_scd2_watermark só pode executar em *_dev durante hardening. Recebido: {CATALOG}")

if not spark.catalog.tableExists(CONTROL_TABLE):
    raise Exception(f"Tabela de controle não existe: {CONTROL_TABLE}")

rows = spark.table(CONTROL_TABLE).filter(F.col("entity") == ENTITY).collect()
if len(rows) != 1:
    raise Exception(f"Esperada exatamente 1 linha de watermark para {ENTITY}; encontrado={len(rows)}")

row = rows[0]
committed = row["last_processed_snapshot"]
candidate = row["candidate_snapshot"]
status = row["status"]
latest_bronze = spark.table(BRONZE).agg(F.max("ingestion_date")).collect()[0][0]

print("\n=== SCD2 — COMMIT WATERMARK ===")
print(f"entity:     {ENTITY}")
print(f"control:    {CONTROL_TABLE}")
print(f"committed:  {committed}")
print(f"candidate:  {candidate}")
print(f"status:     {status}")
print(f"Bronze max: {latest_bronze}\n")

if status == "COMMITTED" and candidate is None:
    print("✅ Nenhum candidate pendente. Watermark já está committed; nada a fazer.")
elif status != "PENDING_VALIDATION" or candidate is None:
    raise Exception(f"Estado inválido para commit: status={status}, candidate={candidate}")
else:
    if committed is not None and candidate < committed:
        raise Exception(f"Candidate {candidate} é anterior ao committed {committed}")
    if candidate > latest_bronze:
        raise Exception(f"Candidate {candidate} está à frente da Bronze {latest_bronze}")

    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET last_processed_snapshot = candidate_snapshot,
            candidate_snapshot = NULL,
            status = 'COMMITTED',
            updated_at = current_timestamp()
        WHERE entity = '{ENTITY}'
    """)

    final_row = spark.table(CONTROL_TABLE).filter(F.col("entity") == ENTITY).collect()[0]
    print(f"✅ Watermark promovido: {committed} -> {final_row['last_processed_snapshot']}")
    print("candidate_snapshot limpo e status=COMMITTED")
