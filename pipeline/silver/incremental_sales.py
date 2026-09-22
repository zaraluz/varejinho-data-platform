# Databricks notebook source
# pipeline/silver/incremental_sales.py
# Runtime incremental D+1 de Bronze venda -> Silver venda.
#
# Regras:
# - lê somente (committed, mature_cutoff]
# - nunca consome a partição ainda aberta
# - Schema Drift canônico antes de Contracts/MERGE
# - MERGE por id: update + insert, sem delete por ausência
# - grava candidate=PENDING_VALIDATION; commit ocorre em task separada
# - Data Contracts via engine canônico em quality/contract_engine.py

from datetime import date
import importlib.util

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
MATURE_CUTOFF_OVERRIDE = job_param("mature_cutoff_override", "")

BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.venda")
SILVER = job_param("silver_table", f"{CATALOG}.silver.venda")
QUARANTINE = job_param(
    "quarantine_table",
    f"{CATALOG}.silver._quarantine_venda",
)
HISTORY = job_param(
    "quarantine_history_table",
    f"{CATALOG}.silver._quarantine_history_venda",
)

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"incremental_sales só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
    )

CONTRACT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_runtime.py"
_runtime_spec = importlib.util.spec_from_file_location(
    "varejinho_contract_runtime", CONTRACT_RUNTIME_PATH
)
if _runtime_spec is None or _runtime_spec.loader is None:
    raise ImportError(f"Não foi possível carregar contract runtime: {CONTRACT_RUNTIME_PATH}")
_contract_runtime_module = importlib.util.module_from_spec(_runtime_spec)
_runtime_spec.loader.exec_module(_contract_runtime_module)
SilverContractRuntime = _contract_runtime_module.SilverContractRuntime
CONTRACTS = SilverContractRuntime(
    spark=spark,
    catalog=CATALOG,
    bundle_files_path=BUNDLE_FILES_PATH,
)
VALIDATOR = CONTRACTS.validator("venda", ["id"])

DRIFT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_runtime.py"
_drift_spec = importlib.util.spec_from_file_location(
    "varejinho_schema_drift_runtime_sales", DRIFT_RUNTIME_PATH
)
if _drift_spec is None or _drift_spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema drift runtime: {DRIFT_RUNTIME_PATH}")
_drift_module = importlib.util.module_from_spec(_drift_spec)
_drift_spec.loader.exec_module(_drift_module)
SilverSchemaDriftRuntime = _drift_module.SilverSchemaDriftRuntime
DRIFT = SilverSchemaDriftRuntime(
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
    bundle_files_path=BUNDLE_FILES_PATH,
)


def latest_mature_partition():
    if MATURE_CUTOFF_OVERRIDE:
        cutoff = date.fromisoformat(MATURE_CUTOFF_OVERRIDE)
        print(f"[venda] mature_cutoff_override={cutoff}")
        return cutoff

    if BRONZE != f"{CATALOG}.bronze.venda":
        raise Exception(
            "venda sandbox com bronze_table override exige mature_cutoff_override"
        )

    source = f"{BRONZE_SOURCE_CATALOG}.bronze.venda"
    maturity = spark.sql(f"""
        SELECT
            ingestion_date,
            MIN(to_date(_metadata.file_modification_time)) AS min_modified_date
        FROM {source}
        GROUP BY ingestion_date
    """)

    return (
        maturity
        .filter(F.col("min_modified_date") > F.col("ingestion_date"))
        .agg(F.max("ingestion_date").alias("mature_cutoff"))
        .collect()[0]["mature_cutoff"]
    )


def transformar(df):
    return (
        df
        .withColumn(
            "valortotal",
            F.regexp_replace("valortotal", ",", ".").cast("decimal(14,2)"),
        )
        .withColumn(
            "quantidade",
            F.regexp_replace("quantidade", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "custocomimposto",
            F.regexp_replace("custocomimposto", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "custosemimposto",
            F.regexp_replace("custosemimposto", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "customediocomimposto",
            F.regexp_replace("customediocomimposto", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "customediosemimposto",
            F.regexp_replace("customediosemimposto", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "piscofins",
            F.regexp_replace("piscofins", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "piscofinscredito",
            F.regexp_replace("piscofinscredito", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "icmscredito",
            F.regexp_replace("icmscredito", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "icmsdebito",
            F.regexp_replace("icmsdebito", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "precovenda",
            F.regexp_replace("precovenda", ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "data",
            F.to_timestamp("data", "yyyy/MM/dd HH:mm:ss.SSS"),
        )
        .withColumn("ano", F.year("data"))
        .withColumn("mes", F.month("data"))
        .withColumnRenamed("valortotal", "valor_total")
    )


if not spark.catalog.tableExists(BRONZE):
    raise Exception(f"Bronze venda ausente: {BRONZE}")
if not spark.catalog.tableExists(SILVER):
    raise Exception(f"Silver venda baseline ausente: {SILVER}")
if not spark.catalog.tableExists(CONTROL_TABLE):
    raise Exception(f"Tabela de controle ausente: {CONTROL_TABLE}")

states = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity") == "venda")
    .collect()
)
if len(states) != 1:
    raise Exception(f"venda: watermark esperado=1; encontrado={len(states)}")

state = states[0]
committed = state["last_processed_snapshot"]
candidate = state["candidate_snapshot"]
status = state["status"]

if status == "PENDING_VALIDATION" and candidate is not None:
    print(
        f"ℹ️ venda: candidate={candidate} já PENDING_VALIDATION. "
        "APPLY anterior preservado; não reaplicando MERGE."
    )
    dbutils.notebook.exit("PENDING_VALIDATION_RESUME")

if status != "COMMITTED" or candidate is not None:
    raise Exception(
        f"venda: estado inicial inválido: committed={committed}, "
        f"candidate={candidate}, status={status}"
    )

mature_cutoff = latest_mature_partition()
bronze_max = spark.table(BRONZE).agg(F.max("ingestion_date")).collect()[0][0]

print("\n=== D7C — VENDA INCREMENTAL APPLY ===")
print(f"committed:          {committed}")
print(f"mature_cutoff:      {mature_cutoff}")
print(f"Bronze max visível: {bronze_max}")

if mature_cutoff is None:
    print("✅ venda: nenhuma partição madura disponível; no-op.")
    dbutils.notebook.exit("NO_MATURE_PARTITION")

if committed is not None and committed > mature_cutoff:
    raise Exception(
        f"venda: committed={committed} está à frente do mature_cutoff={mature_cutoff}"
    )

pending = spark.table(BRONZE)
if committed is not None:
    pending = pending.filter(F.col("ingestion_date") > F.lit(committed))
pending = pending.filter(F.col("ingestion_date") <= F.lit(mature_cutoff))

snapshots = [
    r["ingestion_date"]
    for r in (
        pending.select("ingestion_date").distinct()
        .orderBy("ingestion_date")
        .collect()
    )
]

print(f"pending mature snapshots: {snapshots}")

if not snapshots:
    print("✅ venda: nenhum snapshot maduro pendente; no-op.")
    dbutils.notebook.exit("NO_PENDING_MATURE_SNAPSHOT")

typed = transformar(pending)

# Schema Drift vem antes de Contracts/MERGE:
# - additive permitido é registrado e projetado para o baseline aceito;
# - breaking drift persiste evento e bloqueia antes de qualquer mutação Silver.
typed_accepted, drift_report = DRIFT.evaluate("venda", typed)

# Valida todo o lote maduro. Unicidade é por id+snapshot; depois escolhemos
# o último estado VÁLIDO por id, preservando update/insert/quarantine históricos.
source, invalid, contract_report = CONTRACTS.validate_snapshot_history(
    VALIDATOR,
    typed_accepted,
    ["id"],
)
CONTRACTS.log_report("venda", contract_report)

(
    DeltaTable.forName(spark, SILVER).alias("t")
    .merge(source.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

if spark.catalog.tableExists(QUARANTINE):
    spark.sql(f"TRUNCATE TABLE {QUARANTINE}")

invalid_count = invalid.count()
if invalid_count:
    invalid.write.format("delta").mode("append").saveAsTable(QUARANTINE)
    (
        invalid.withColumn("_quarantined_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(HISTORY)
    )

spark.sql(f"""
    UPDATE {CONTROL_TABLE}
    SET candidate_snapshot = DATE '{mature_cutoff}',
        status = 'PENDING_VALIDATION',
        updated_at = current_timestamp()
    WHERE entity = 'venda'
      AND status = 'COMMITTED'
      AND candidate_snapshot IS NULL
""")

final_state = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity") == "venda")
    .collect()[0]
)

if (
    final_state["candidate_snapshot"] != mature_cutoff
    or final_state["status"] != "PENDING_VALIDATION"
):
    raise Exception("venda: falha ao registrar candidate_snapshot")

print(
    f"✅ APPLY venda concluído | snapshots={len(snapshots)} "
    f"| drift={drift_report['classification']} "
    f"| contract rows={contract_report['total']:,} "
    f"| source final={source.count():,} | quarantine={invalid_count:,}"
)
print(
    f"✅ candidate={mature_cutoff}; committed continua={committed}; "
    "status=PENDING_VALIDATION"
)
print("ℹ️ Ausência de id em snapshot novo não executa DELETE.")