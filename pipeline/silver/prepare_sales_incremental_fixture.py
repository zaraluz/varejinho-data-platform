# Databricks notebook source
# pipeline/silver/prepare_sales_incremental_fixture.py
# Gate D7C — fixture para provar venda incremental D+1.
# Também cria explicitamente o baseline de Schema Drift dentro do sandbox D7C.

import importlib.util

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")
DRIFT_CONTROL_ROOT = CONTROL_ROOT if CONTROL_ROOT.endswith("/d7c") else f"{CONTROL_ROOT}/d7c"

BRONZE = f"{CATALOG}.control._d7c_venda_bronze"
SILVER = f"{CATALOG}.silver._d7c_venda"
CONTROL = f"{CATALOG}.control._d7c_fact_watermark"
QUAR = f"{CATALOG}.silver._d7c_quarantine_venda"
HIST = f"{CATALOG}.silver._d7c_quarantine_history_venda"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D7C só pode executar em *_dev. Recebido: {CATALOG}")

# Registry isolado da fixture. Nunca remove o registry real em .../_control/dev/schema_registry.
dbutils.fs.rm(DRIFT_CONTROL_ROOT, True)

for table in [HIST, QUAR, SILVER, CONTROL, BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

rows = [
    # id, data, loja, produto, valor, qtd, 9 métricas, ingestion_date
    ("920001", "2026/09/01 10:00:00.000", "1", "101", "10,00", "1,000", "5,000", "4,000", "5,000", "4,000", "0,100", "0,000", "0,000", "0,100", "10,000", "2026-09-01"),
    ("920002", "2026/09/01 11:00:00.000", "1", "102", "20,00", "2,000", "8,000", "7,000", "8,000", "7,000", "0,200", "0,000", "0,000", "0,200", "10,000", "2026-09-01"),

    # D2 madura: 920002 repetido sem churn; 920003 novo; 920005 inválido.
    ("920002", "2026/09/01 11:00:00.000", "1", "102", "20,00", "2,000", "8,000", "7,000", "8,000", "7,000", "0,200", "0,000", "0,000", "0,200", "10,000", "2026-09-02"),
    ("920003", "2026/09/02 09:00:00.000", "2", "103", "30,00", "1,000", "12,000", "11,000", "12,000", "11,000", "0,300", "0,000", "0,000", "0,300", "30,000", "2026-09-02"),
    ("920005", "2026/09/02 09:30:00.000", "2", "105", "-1,00", "1,000", "1,000", "1,000", "1,000", "1,000", "0,000", "0,000", "0,000", "0,000", "1,000", "2026-09-02"),

    # D3 aberta: não pode entrar.
    ("920002", "2026/09/01 11:00:00.000", "1", "102", "20,00", "2,000", "8,000", "7,000", "8,000", "7,000", "0,200", "0,000", "0,000", "0,200", "10,000", "2026-09-03"),
    ("920003", "2026/09/02 09:00:00.000", "2", "103", "30,00", "1,000", "12,000", "11,000", "12,000", "11,000", "0,300", "0,000", "0,000", "0,300", "30,000", "2026-09-03"),
    ("920004", "2026/09/03 08:00:00.000", "2", "104", "40,00", "1,000", "15,000", "14,000", "15,000", "14,000", "0,400", "0,000", "0,000", "0,400", "40,000", "2026-09-03"),
]

schema = """
id string,
data string,
id_loja string,
id_produto string,
valortotal string,
quantidade string,
custocomimposto string,
custosemimposto string,
customediocomimposto string,
customediosemimposto string,
piscofins string,
piscofinscredito string,
icmscredito string,
icmsdebito string,
precovenda string,
ingestion_date string
"""

bronze = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("ingestion_date", F.to_date("ingestion_date"))
)
bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE)

baseline = (
    bronze
    .filter(F.col("ingestion_date") == F.lit("2026-09-01").cast("date"))
    .withColumn("valortotal", F.regexp_replace("valortotal", ",", ".").cast("decimal(14,2)"))
    .withColumn("quantidade", F.regexp_replace("quantidade", ",", ".").cast("decimal(14,3)"))
    .withColumn("custocomimposto", F.regexp_replace("custocomimposto", ",", ".").cast("decimal(14,3)"))
    .withColumn("custosemimposto", F.regexp_replace("custosemimposto", ",", ".").cast("decimal(14,3)"))
    .withColumn("customediocomimposto", F.regexp_replace("customediocomimposto", ",", ".").cast("decimal(14,3)"))
    .withColumn("customediosemimposto", F.regexp_replace("customediosemimposto", ",", ".").cast("decimal(14,3)"))
    .withColumn("piscofins", F.regexp_replace("piscofins", ",", ".").cast("decimal(14,3)"))
    .withColumn("piscofinscredito", F.regexp_replace("piscofinscredito", ",", ".").cast("decimal(14,3)"))
    .withColumn("icmscredito", F.regexp_replace("icmscredito", ",", ".").cast("decimal(14,3)"))
    .withColumn("icmsdebito", F.regexp_replace("icmsdebito", ",", ".").cast("decimal(14,3)"))
    .withColumn("precovenda", F.regexp_replace("precovenda", ",", ".").cast("decimal(14,3)"))
    .withColumn("data", F.to_timestamp("data", "yyyy/MM/dd HH:mm:ss.SSS"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumnRenamed("valortotal", "valor_total")
)
baseline.write.format("delta").mode("overwrite").saveAsTable(SILVER)

# Bootstrap explícito do baseline SOMENTE no registry sandbox da fixture.
ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_engine.py"
_spec = importlib.util.spec_from_file_location("d7c_schema_drift_engine", ENGINE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema drift engine: {ENGINE_PATH}")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
SchemaDriftEngine = _module.SchemaDriftEngine

SchemaDriftEngine(
    dbutils=dbutils,
    control_root=DRIFT_CONTROL_ROOT,
).bootstrap_baseline(
    entity="venda",
    df=spark.table(SILVER),
    approved_by="fixture:d7c",
    reason="D7C sandbox baseline from initial committed Silver fixture",
)

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
    VALUES ('venda', DATE '2026-09-01', NULL, 'COMMITTED', current_timestamp())
""")

print("\n=== D7C — PREPARE VENDA INCREMENTAL FIXTURE ===")
print("D1 committed; D2 madura; D3 aberta.")
print("D2 inclui update idempotente, insert novo e 1 inválido.")
print("Expected: somente D2 entra; D3 fica intocada.")
print(f"Drift registry: {DRIFT_CONTROL_ROOT}/schema_registry/venda.json")
print("✅ Sandbox criado com baseline de drift explícito e isolado.")
