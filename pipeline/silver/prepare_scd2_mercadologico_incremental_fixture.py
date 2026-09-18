# Databricks notebook source
# pipeline/silver/prepare_scd2_mercadologico_incremental_fixture.py
# Gate B8D — fixture incremental do mercadologico para provar o engine genérico.

from pyspark.sql import functions as F

def job_param(nome, default):
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default

CATALOG = job_param("catalog", "varejinho_dev")
FULL_BRONZE = f"{CATALOG}.control._b8d_mercadologico_full_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._b8d_mercadologico_baseline_bronze"
SILVER = f"{CATALOG}.silver._b8d_mercadologico_incremental"
CONTROL = f"{CATALOG}.control._b8d_mercadologico_watermark"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B8D só pode executar em *_dev. Recebido: {CATALOG}")

for t in [CONTROL, SILVER, BASELINE_BRONZE, FULL_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

rows = [
    ("9001","1","1","0","0","0","2","ARROZ","2026-09-01"),
    ("9001","1","1","0","0","0","2","ARROZ E CEREAIS","2026-09-02"),  # Type 1
    ("9001","1","9","0","0","0","2","ARROZ E CEREAIS","2026-09-03"),  # Type 2
    ("9001","1","9","7","0","0","3","ARROZ E CEREAIS","2026-09-04"),  # Type 2
    ("9002","5","6","3","0","0","3","REFRIGERANTE LATA","2026-09-03"), # novo id
    ("9002","5","6","3","0","0","3","REFRIGERANTES LATA","2026-09-04"), # Type 1
]
schema = """
id string,
mercadologico1 string,
mercadologico2 string,
mercadologico3 string,
mercadologico4 string,
mercadologico5 string,
nivel string,
descricao string,
ingestion_date string
"""

full = spark.createDataFrame(rows, schema).withColumn("ingestion_date", F.to_date("ingestion_date"))
baseline = full.filter(F.col("ingestion_date") <= F.lit("2026-09-02").cast("date"))

full.write.format("delta").mode("overwrite").saveAsTable(FULL_BRONZE)
baseline.write.format("delta").mode("overwrite").saveAsTable(BASELINE_BRONZE)

spark.sql(f"""
    CREATE TABLE {CONTROL} (
        entity STRING,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING,
        updated_at TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CONTROL}
    VALUES ('mercadologico', DATE '2026-09-02', NULL, 'COMMITTED', current_timestamp())
""")

print("\n=== GATE B8D — PREPARE MERCADOLOGICO INCREMENTAL FIXTURE ===")
print(f"Bronze baseline: {BASELINE_BRONZE}")
print(f"Bronze completa: {FULL_BRONZE}")
print(f"Silver sandbox:  {SILVER}")
print(f"Control sandbox: {CONTROL}")
print(f"baseline rows:    {baseline.count()}")
print(f"full rows:        {full.count()}")
print("watermark inicial: 2026-09-02")
print("pendentes: 2026-09-03 e 2026-09-04")
print("✅ Fixture incremental preparada; nenhum dado real foi alterado.")
