# Databricks notebook source
# pipeline/silver/validate_mature_incremental_sales.py
# Gate D7C — valida somente o lote maduro novo de venda antes do commit.
# Expected e runtime usam os mesmos engines canônicos de Drift + Contracts.

from functools import reduce
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
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.venda")
SILVER = job_param("silver_table", f"{CATALOG}.silver.venda")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"validate_mature_incremental_sales só pode executar em *_dev. "
        f"Recebido: {CATALOG}"
    )

CONTRACT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_runtime.py"
_runtime_spec = importlib.util.spec_from_file_location(
    "varejinho_contract_runtime_validate_sales", CONTRACT_RUNTIME_PATH
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
    "varejinho_schema_drift_runtime_validate_sales", DRIFT_RUNTIME_PATH
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


def transformar(df):
    return (
        df
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


rows = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity") == "venda")
    .collect()
)
if len(rows) != 1:
    raise Exception(f"venda: watermark esperado=1; encontrado={len(rows)}")

state = rows[0]
committed = state["last_processed_snapshot"]
candidate = state["candidate_snapshot"]
status = state["status"]

if status == "COMMITTED" and candidate is None:
    print("ℹ️ venda: sem candidate pendente; nada a validar.")
    dbutils.notebook.exit("NO_PENDING_CANDIDATE")

if status != "PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"venda: estado inválido para validação: committed={committed}, "
        f"candidate={candidate}, status={status}"
    )

batch = spark.table(BRONZE)
if committed is not None:
    batch = batch.filter(F.col("ingestion_date") > F.lit(committed))
batch = batch.filter(F.col("ingestion_date") <= F.lit(candidate))

typed_batch = transformar(batch)
accepted_batch, drift_report = DRIFT.evaluate("venda", typed_batch)
expected, _, contract_report = CONTRACTS.validate_snapshot_history(
    VALIDATOR,
    accepted_batch,
    ["id"],
)
CONTRACTS.log_report("validate:venda", contract_report)

actual = spark.table(SILVER)

e_schema = {f.name: f.dataType.simpleString() for f in expected.schema.fields}
a_schema = {f.name: f.dataType.simpleString() for f in actual.schema.fields}
schema_ok = e_schema == a_schema

e_dup = expected.groupBy("id").count().filter("count > 1").count()
a_dup = actual.groupBy("id").count().filter("count > 1").count()

expected_keys = expected.select("id")
actual_keys = actual.select("id")

missing = expected_keys.join(
    actual_keys,
    on="id",
    how="left_anti",
).count()

future = actual.filter(
    F.col("ingestion_date") > F.lit(candidate)
).count()

mismatches = None
if schema_ok and e_dup == 0 and a_dup == 0:
    cols = expected.columns
    nonkeys = [c for c in cols if c != "id"]

    e = expected.alias("e")
    a = actual.alias("a")
    joined = e.join(
        a,
        F.col("e.id").eqNullSafe(F.col("a.id")),
        "inner",
    )

    if nonkeys:
        diff = reduce(
            lambda acc, c: acc | (~F.col(f"e.{c}").eqNullSafe(F.col(f"a.{c}"))),
            nonkeys[1:],
            ~F.col(f"e.{nonkeys[0]}").eqNullSafe(
                F.col(f"a.{nonkeys[0]}")
            ),
        )
        mismatches = joined.filter(diff).count()
    else:
        mismatches = 0

ok = (
    schema_ok
    and e_dup == 0
    and a_dup == 0
    and missing == 0
    and future == 0
    and mismatches == 0
)

print("\n=== D7C — VALIDATE MATURE VENDA INCREMENTAL ===")
print(f"committed:          {committed}")
print(f"candidate:          {candidate}")
print(f"drift:              {drift_report['classification']}")
print(f"batch expected rows:{expected.count():,}")
print(f"Silver total rows:  {actual.count():,}")
print(f"schema exact:       {schema_ok}")
print(f"duplicate ids:      expected={e_dup:,} | actual={a_dup:,}")
print(f"missing batch ids:  {missing:,}")
print(f"value mismatches:   {mismatches}")
print(f"rows > candidate:   {future:,}")
print(f"RESULT:             {'✅ PASS' if ok else '❌ FAIL'}")

if not ok:
    if missing:
        print("\nAmostra de ids do lote ausentes na Silver:")
        expected_keys.join(
            actual_keys,
            on="id",
            how="left_anti",
        ).limit(10).show(truncate=False)

    raise Exception(
        f"venda: lote maduro incremental divergiu para candidate={candidate}"
    )

print("✅ Lote incremental de venda aprovado; watermark ainda não committed.")