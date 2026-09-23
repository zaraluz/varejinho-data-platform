# Databricks notebook source
# ops/repair/verify_sales_maturity_repair.py
# Gate D7B — verificação final do repair + watermark de venda.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
CONTROL = f"{CATALOG}.control.fact_watermark"
SILVER = f"{CATALOG}.silver.venda"

source = f"{BRONZE_SOURCE_CATALOG}.bronze.venda"
maturity = spark.sql(f"""
    SELECT
        ingestion_date,
        MIN(to_date(_metadata.file_modification_time)) AS min_modified_date
    FROM {source}
    GROUP BY ingestion_date
""")

mature_cutoff = (
    maturity
    .filter(F.col("min_modified_date") > F.col("ingestion_date"))
    .agg(F.max("ingestion_date").alias("mature_cutoff"))
    .collect()[0]["mature_cutoff"]
)

state = spark.table(CONTROL).filter(F.col("entity") == "venda").collect()[0]
future = (
    spark.table(SILVER)
    .filter(F.col("ingestion_date") > F.lit(mature_cutoff))
    .count()
)

ok = (
    state["last_processed_snapshot"] == mature_cutoff
    and state["candidate_snapshot"] is None
    and state["status"] == "COMMITTED"
    and future == 0
)

print("\n=== RESULTADO D7B — VENDA ===")
print(f"committed:     {state['last_processed_snapshot']}")
print(f"mature_cutoff: {mature_cutoff}")
print(f"candidate:     {state['candidate_snapshot']}")
print(f"status:        {state['status']}")
print(f"future_rows:   {future}")

if not ok:
    raise Exception("Gate D7B falhou na verificação final")

print("✅ venda reparada e alinhada ao mature_cutoff.")
print("✅ Baseline pronto para runtime incremental D+1.")
