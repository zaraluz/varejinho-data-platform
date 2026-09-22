# Databricks notebook source
# pipeline/silver/transform_reference_dimensions.py
# Dimensões SCD1/domínios + curvaabc snapshot.
# IMPORTANTE: produto, fornecedor e mercadologico NÃO são tratados aqui.
# Esses três SCD2 pertencem ao runtime incremental_scd2.py.
# Schema Drift: preflight de todas as entidades ANTES de qualquer write.

import importlib.util

from pyspark.sql import functions as F
from delta.tables import DeltaTable


def job_param(nome: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(nome)
        return value if value else default
    except Exception:
        return default


def resolve_bundle_files_path() -> str:
    explicit = job_param("bundle_files_path", "")
    if explicit:
        return explicit.rstrip("/")

    try:
        raw = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        workspace_path = raw if raw.startswith("/Workspace/") else f"/Workspace{raw}"
        marker = "/pipeline/silver/transform_reference_dimensions"
        if marker in workspace_path:
            return workspace_path.split(marker, 1)[0]
    except Exception:
        pass

    # Fallback somente para compatibilidade manual antiga.
    return "/Workspace/Users/<USER>/varejinho-data-platform"


CATALOG = job_param("catalog", "varejinho_dev")
BUNDLE_FILES_PATH = resolve_bundle_files_path()
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"transform_reference_dimensions só pode executar em *_dev durante hardening. "
        f"Recebido: {CATALOG}"
    )

DRIFT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_runtime.py"
_drift_spec = importlib.util.spec_from_file_location(
    "varejinho_schema_drift_runtime_reference", DRIFT_RUNTIME_PATH
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

SCD1_TABELAS = [
    "loja", "produtofornecedor", "tipocurvaabc", "tipomotivoperda", "tipopedido", "tipopromocao",
    "situacaocadastro", "situacaonotaentrada", "situacaopagarfornecedorparcela",
    "situacaopagaroutrasdespesas", "situacaopedido", "tipoembalagem", "tipoentrada",
    "tipofornecedor", "tipomercadoria", "tipomovimentacao", "tipooferta",
    "tipopagamento", "tipoplanoconta",
]

print("\n=== SILVER — REFERENCE DIMENSIONS / SNAPSHOTS ===")
print(f"Catalog: {CATALOG}")
print(f"Bundle files path: {BUNDLE_FILES_PATH}")
print("SCD2 excluídos deste notebook: produto, fornecedor, mercadologico")
print("Schema Drift: preflight completo antes de qualquer write\n")

# -----------------------------------------------------------------------------
# PHASE 1 — PREPARE + DRIFT PREFLIGHT (read-only para Silver)
# Se qualquer entidade tiver breaking drift, o notebook falha antes de sobrescrever
# qualquer reference dimension.
# -----------------------------------------------------------------------------
prepared_scd1 = {}
preflight_results = []

for tabela in SCD1_TABELAS:
    bronze = f"{CATALOG}.bronze.{tabela}"
    silver = f"{CATALOG}.silver.{tabela}"

    if not spark.catalog.tableExists(bronze):
        raise Exception(f"{tabela}: Bronze ausente: {bronze}")
    if not spark.catalog.tableExists(silver):
        raise Exception(f"{tabela}: Silver baseline ausente: {silver}")

    ultima = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]
    if ultima is None:
        raise Exception(f"{tabela}: Bronze sem ingestion_date válido")

    observed = spark.table(bronze).where(F.col("ingestion_date") == F.lit(ultima))
    accepted, drift_report = DRIFT.evaluate(tabela, observed)
    prepared_scd1[tabela] = {
        "df": accepted,
        "silver": silver,
        "snapshot": ultima,
        "drift": drift_report,
    }
    preflight_results.append(
        f"✅ {tabela}: drift={drift_report['classification']} | action={drift_report['action']}"
    )

# CURVAABC: o baseline é o schema Silver tipado, então o drift é avaliado depois
# dos casts/snapshot_date e antes do MERGE.
curva_bronze = f"{CATALOG}.bronze.curvaabc"
curva_silver = f"{CATALOG}.silver.curvaabc"
if not spark.catalog.tableExists(curva_bronze):
    raise Exception(f"curvaabc: Bronze ausente: {curva_bronze}")
if not spark.catalog.tableExists(curva_silver):
    raise Exception(f"curvaabc: Silver baseline ausente: {curva_silver}")

curva_raw = spark.table(curva_bronze)
curva_typed = (
    curva_raw.withColumn("id", F.col("id").cast("bigint"))
      .withColumn("id_loja", F.col("id_loja").cast("int"))
      .withColumn("id_produto", F.col("id_produto").cast("int"))
      .withColumn("quantidade", F.regexp_replace(F.col("quantidade"), ",", ".").cast("decimal(14,3)"))
      .withColumn("valortotal", F.regexp_replace(F.col("valortotal"), ",", ".").cast("decimal(14,2)"))
      .withColumn("lucro", F.regexp_replace(F.col("lucro"), ",", ".").cast("decimal(14,2)"))
      .withColumn("id_tipocurvaabc_nivel1", F.col("id_tipocurvaabc_nivel1").cast("int"))
      .withColumn("id_tipocurvaabc_nivel2", F.col("id_tipocurvaabc_nivel2").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico1_nivel1", F.col("id_tipocurvaabcmercadologico1_nivel1").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico1_nivel2", F.col("id_tipocurvaabcmercadologico1_nivel2").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico2_nivel1", F.col("id_tipocurvaabcmercadologico2_nivel1").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico2_nivel2", F.col("id_tipocurvaabcmercadologico2_nivel2").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico3_nivel1", F.col("id_tipocurvaabcmercadologico3_nivel1").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico3_nivel2", F.col("id_tipocurvaabcmercadologico3_nivel2").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico4_nivel1", F.col("id_tipocurvaabcmercadologico4_nivel1").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico4_nivel2", F.col("id_tipocurvaabcmercadologico4_nivel2").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico5_nivel1", F.col("id_tipocurvaabcmercadologico5_nivel1").cast("int"))
      .withColumn("id_tipocurvaabcmercadologico5_nivel2", F.col("id_tipocurvaabcmercadologico5_nivel2").cast("int"))
      .withColumn("snapshot_date", F.col("ingestion_date").cast("date"))
)
curva_accepted, curva_drift = DRIFT.evaluate("curvaabc", curva_typed)
preflight_results.append(
    f"✅ curvaabc: drift={curva_drift['classification']} | action={curva_drift['action']}"
)

print("=== SCHEMA DRIFT PREFLIGHT ===")
for line in preflight_results:
    print(line)
print("✅ 20/20 entidades passaram o preflight; nenhum write Silver ocorreu até aqui.\n")

# -----------------------------------------------------------------------------
# PHASE 2 — WRITES
# Só executa se todas as 20 entidades tiverem passado o drift preflight.
# -----------------------------------------------------------------------------
resultados = []

for tabela in SCD1_TABELAS:
    item = prepared_scd1[tabela]
    (
        item["df"].write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(item["silver"])
    )

    count = spark.table(item["silver"]).count()
    resultados.append(
        f"✅ {tabela} SCD1: {count:,} | snapshot={item['snapshot']} "
        f"| drift={item['drift']['classification']}"
    )

(
    DeltaTable.forName(spark, curva_silver).alias("t")
    .merge(
        curva_accepted.alias("s"),
        "t.id_produto = s.id_produto "
        "AND t.id_loja = s.id_loja "
        "AND t.snapshot_date = s.snapshot_date",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

curva_out = spark.table(curva_silver)
count = curva_out.count()
snapshots = curva_out.select("snapshot_date").distinct().count()
produtos = curva_out.select("id_produto").distinct().count()
resultados.append(
    f"✅ curvaabc snapshot: {count:,} linhas | {snapshots} snapshots | "
    f"{produtos:,} produtos | drift={curva_drift['classification']}"
)

print("\n=== RESULTADO ===")
for r in resultados:
    print(r)

print("\n✅ Reference dimensions concluídas sem tocar nas três dimensões SCD2.")
print("✅ Breaking drift teria bloqueado antes do primeiro write Silver.")
