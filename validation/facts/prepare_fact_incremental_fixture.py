# Databricks notebook source
# validation/facts/prepare_fact_incremental_fixture.py
# Gate D4 — fixture sandbox para provar update/insert/no-delete/quarantine/watermark.
# Também cria explicitamente o baseline de Schema Drift dentro do sandbox D4.

import importlib.util
from datetime import date

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
DRIFT_CONTROL_ROOT = CONTROL_ROOT if CONTROL_ROOT.endswith("/d4") else f"{CONTROL_ROOT}/d4"

BRONZE = f"{CATALOG}.control._d4_pedido_bronze"
SILVER = f"{CATALOG}.silver._d4_pedido"
CONTROL = f"{CATALOG}.control._d4_fact_watermark"
QUAR = f"{CATALOG}.silver._d4_quarantine_pedido"
HIST = f"{CATALOG}.silver._d4_quarantine_history_pedido"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D4 só pode executar em *_dev. Recebido: {CATALOG}")

# A fixture precisa de registry próprio. Remover o sandbox anterior é parte do setup,
# nunca toca no registry real em .../_control/dev/schema_registry.
dbutils.fs.rm(DRIFT_CONTROL_ROOT, True)

for table in [HIST, QUAR, SILVER, CONTROL, BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

rows = [
    # D1 baseline: 900001 e 900002.
    ("900001", "10", "1", "2026/09/01 10:00:00.000", "1", "1", "2026-09-01"),
    ("900002", "20", "1", "2026/09/01 11:00:00.000", "1", "1", "2026-09-01"),

    # D2: 900001 some (deve ser preservado), 900002 muda, 900003 entra.
    ("900002", "20", "1", "2026/09/01 11:00:00.000", "1", "2", "2026-09-02"),
    ("900003", "30", "2", "2026/09/02 09:00:00.000", "2", "1", "2026-09-02"),

    # D3: 900002 muda de novo; 900003 permanece; 900004 é inválido.
    ("900002", "20", "1", "2026/09/01 11:00:00.000", "1", "3", "2026-09-03"),
    ("900003", "30", "2", "2026/09/02 09:00:00.000", "2", "1", "2026-09-03"),
    ("900004", None, "2", "2026/09/03 08:00:00.000", "2", "1", "2026-09-03"),
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

# Bootstrap explícito do baseline SOMENTE no registry sandbox da fixture.
ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_engine.py"
_spec = importlib.util.spec_from_file_location("d4_schema_drift_engine", ENGINE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema drift engine: {ENGINE_PATH}")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
SchemaDriftEngine = _module.SchemaDriftEngine

SchemaDriftEngine(
    dbutils=dbutils,
    control_root=DRIFT_CONTROL_ROOT,
).bootstrap_baseline(
    entity="pedido",
    df=spark.table(SILVER),
    approved_by="fixture:d4",
    reason="D4 sandbox baseline from initial committed Silver fixture",
)

MANIFEST_ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest.py"
_manifest_spec = importlib.util.spec_from_file_location(
    "d4_partition_manifest_engine",
    MANIFEST_ENGINE_PATH,
)
if _manifest_spec is None or _manifest_spec.loader is None:
    raise ImportError(
        f"Não foi possível carregar partition manifest engine: {MANIFEST_ENGINE_PATH}"
    )
_manifest_module = importlib.util.module_from_spec(_manifest_spec)
_manifest_spec.loader.exec_module(_manifest_module)
FactPartitionManifestGuard = _manifest_module.FactPartitionManifestGuard

manifest_bootstrap = FactPartitionManifestGuard(
    spark=spark,
    dbutils=dbutils,
    control_root=DRIFT_CONTROL_ROOT,
).bootstrap(
    entity="pedido",
    source_table=BRONZE,
    committed=date(2026, 9, 1),
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
    VALUES ('pedido', DATE '2026-09-01', NULL, 'COMMITTED', current_timestamp())
""")

print("\n=== GATE D4 — PREPARE FACT INCREMENTAL FIXTURE ===")
print(f"Bronze sandbox: {BRONZE}")
print(f"Silver baseline:{SILVER}")
print(f"Control:        {CONTROL}")
print(f"Drift registry: {DRIFT_CONTROL_ROOT}/schema_registry/pedido.json")
print(
    f"Mutation manifest: rows={manifest_bootstrap['rows']} "
    f"| created={manifest_bootstrap['created']}"
)
print("Cenários: D2/D3 pendentes, update, insert, ausência sem delete e quarentena.")
print("✅ Fixture pronta; baseline de drift criado somente no sandbox D4.")
print("✅ Nenhum dado real foi alterado.")