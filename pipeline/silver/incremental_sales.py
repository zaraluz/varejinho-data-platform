# Databricks notebook source
# pipeline/silver/incremental_sales.py
# Runtime incremental D+1 de Bronze venda -> Silver venda.
#
# Regras:
# - lê somente (committed, mature_cutoff]
# - nunca consome a partição ainda aberta
# - aplica a mesma transformação/contrato do transform_sales.py legado
# - MERGE por id: update + insert, sem delete por ausência
# - grava candidate=PENDING_VALIDATION; commit ocorre em task separada

from datetime import date
import json
import yaml

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
CONTRACT = f"{BUNDLE_FILES_PATH}/contracts/silver/venda.yaml"
REGISTRY = f"{CONTROL_ROOT}/schema_registry"

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"incremental_sales só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
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


def detectar_drift(df):
    schema_atual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    registry_file = f"{REGISTRY}/venda.json"

    try:
        anterior = json.loads(dbutils.fs.head(registry_file))
    except Exception:
        dbutils.fs.put(
            registry_file,
            json.dumps(schema_atual),
            overwrite=True,
        )
        print(f"[venda] Schema baseline criado em {registry_file}")
        return

    novas = sorted(set(schema_atual) - set(anterior))
    removidas = sorted(set(anterior) - set(schema_atual))
    alteradas = {
        c: {"antes": anterior[c], "depois": schema_atual[c]}
        for c in set(schema_atual) & set(anterior)
        if anterior[c] != schema_atual[c]
    }

    if novas or removidas or alteradas:
        print(
            f"[DRIFT] venda: novas={novas} removidas={removidas} "
            f"alteradas={alteradas}"
        )
    else:
        print("[venda] Schema sem alterações.")

    dbutils.fs.put(
        registry_file,
        json.dumps(schema_atual),
        overwrite=True,
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


def aplicar_contrato(df):
    with open(CONTRACT, "r") as f:
        contract = yaml.safe_load(f)

    work = (
        df.withColumn("_invalido", F.lit(False))
          .withColumn("_motivo", F.lit(""))
    )

    for cfg in contract.get("columns", []):
        name = cfg.get("name")
        if name not in work.columns:
            continue

        if not cfg.get("nullable", True):
            work = (
                work.withColumn(
                    "_invalido",
                    F.when(F.col(name).isNull(), True)
                     .otherwise(F.col("_invalido")),
                )
                .withColumn(
                    "_motivo",
                    F.when(
                        F.col(name).isNull(),
                        F.concat(F.col("_motivo"), F.lit(f"|{name} nulo")),
                    ).otherwise(F.col("_motivo")),
                )
            )

        min_val = cfg.get("min")
        if min_val is not None:
            try:
                min_num = float(min_val)
                work = (
                    work.withColumn(
                        "_invalido",
                        F.when(F.col(name).cast("double") < min_num, True)
                         .otherwise(F.col("_invalido")),
                    )
                    .withColumn(
                        "_motivo",
                        F.when(
                            F.col(name).cast("double") < min_num,
                            F.concat(
                                F.col("_motivo"),
                                F.lit(f"|{name} < {min_val}"),
                            ),
                        ).otherwise(F.col("_motivo")),
                    )
                )
            except (TypeError, ValueError):
                pass

    return (
        work.where(~F.col("_invalido")).drop("_invalido", "_motivo"),
        work.where(F.col("_invalido")).drop("_invalido"),
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

# Resume seguro depois de eventual falha no validator.
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

null_ids = pending.filter(F.col("id").isNull()).count()
if null_ids:
    raise Exception(f"venda: {null_ids} linha(s) pendente(s) com id nulo")

dup_groups = (
    pending.groupBy("id", "ingestion_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
if dup_groups:
    raise Exception(
        f"venda: {dup_groups} duplicidade(s) por (id, ingestion_date) no lote"
    )

typed = transformar(pending)
detectar_drift(typed)
valid, invalid = aplicar_contrato(typed)

w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
source = (
    valid.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

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
    f"| source final={source.count():,} | quarantine={invalid_count:,}"
)
print(
    f"✅ candidate={mature_cutoff}; committed continua={committed}; "
    "status=PENDING_VALIDATION"
)
print("ℹ️ Ausência de id em snapshot novo não executa DELETE.")
