# Databricks notebook source
# pipeline/silver/commit_scd2_product_watermark.py
# Gate B5 — promove candidate watermark para committed SOMENTE após o Quality Gate.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_TABLE = f"{CATALOG}.control.scd2_watermark"
BRONZE = f"{CATALOG}.bronze.produto"
ENTITY = "produto"

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Proteção de hardening: commit_scd2_product_watermark só pode executar em *_dev. Recebido: {CATALOG}"
    )

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

print("\n=== GATE B5 — COMMIT WATERMARK ===")
print(f"entity:     {ENTITY}")
print(f"committed:  {committed}")
print(f"candidate:  {candidate}")
print(f"status:     {status}")
print(f"Bronze max: {latest_bronze}\n")

if status == "COMMITTED" and candidate is None:
    print("✅ Nenhum candidate pendente. Watermark já está committed; nada a fazer.")
elif status != "PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"Estado de controle inválido para commit: status={status}, candidate={candidate}"
    )
else:
    if candidate < committed:
        raise Exception(f"Candidate {candidate} é anterior ao committed {committed}")
    if candidate > latest_bronze:
        raise Exception(f"Candidate {candidate} está à frente do último snapshot Bronze {latest_bronze}")

    # Esta task só executa porque depende da validação B4. Se B4 falhar,
    # o Databricks Job não chega aqui e last_processed_snapshot permanece intacto.
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
