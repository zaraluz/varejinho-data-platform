# Databricks notebook source
# validation/facts/prepare_fact_maturity_fixture.py
# Gate D6A — sandbox para provar que partição aberta não é processada.

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

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D6A só pode executar em *_dev. Recebido: {CATALOG}")

for table in [HIST, QUAR, SILVER, CONTROL, BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

rows = [
    # D1 já committed.
    ("910001", "10", "1", "2026/09/01 10:00:00.000", "1", "1", "2026-09-01"),
    ("910002", "20", "1", "2026/09/01 11:00:00.000", "1", "1", "2026-09-01"),

    # D2 será a maior partição madura.
    ("910002", "20", "1", "2026/09/01 11:00:00.000", "1", "2", "2026-09-02"),
    ("910003", "30", "2", "2026/09/02 09:00:00.000", "2", "1", "2026-09-02"),

    # D3 existe na Bronze, mas representa partição ainda aberta.
    ("910002", "20", "1", "2026/09/01 11:00:00.000", "1", "3", "2026-09-03"),
    ("910004", "40", "2", "2026/09/03 08:00:00.000", "2", "1", "2026-09-03"),
]

schema = """
id string,
id_fornecedor string,
id_loja string,
datacompra string,
id_tipopedido string,
id_situacaopedido string,
ingestion_date string
"""

bronze = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("ingestion_date", F.to_date("ingestion_date"))
)
bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE)

baseline = (
    bronze.filter(F.col("ingestion_date") == F.lit("2026-09-01").cast("date"))
    .withColumn(
        "datacompra",
        F.to_timestamp(F.col("datacompra"), "yyyy/MM/dd HH:mm:ss.SSS"),
    )
    .withColumn("ano", F.year("datacompra"))
    .withColumn("mes", F.month("datacompra"))
)
baseline.write.format("delta").mode("overwrite").saveAsTable(SILVER)

spark.sql(f"""
    CREATE TABLE {CONTROL} (
        entity STRING NOT NULL,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING NOT NULL,
        updated_at TIMESTAMP NOT NULL
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CONTROL}
    VALUES ('pedido', DATE '2026-09-01', NULL, 'COMMITTED', current_timestamp())
""")

print("\n=== GATE D6A — PREPARE PARTITION MATURITY FIXTURE ===")
print("D1 committed; D2 mature; D3 visible but OPEN.")
print("Expected: runtime processes only D2 and leaves D3 untouched.")
print("✅ Sandbox criado; dados reais intactos.")
