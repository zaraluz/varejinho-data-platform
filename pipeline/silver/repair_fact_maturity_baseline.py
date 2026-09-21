# Databricks notebook source
# pipeline/silver/repair_fact_maturity_baseline.py
# Gate D6B — one-time repair do baseline real após descoberta da regra D+1.
#
# Estratégia:
# - reconstrói o estado esperado usando SOMENTE partições maduras
# - MERGE sem delete para preservar histórico observado
# - remove apenas linhas com ingestion_date > mature_cutoff (dados prematuros/open)
# - NÃO altera committed ainda; grava candidate e REPAIR_PENDING_VALIDATION

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import yaml


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
AUDIT_TABLE = f"{CATALOG}.control.fact_maturity_repair_audit"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D6B só pode executar em *_dev. Recebido: {CATALOG}")

CONFIG = {
    "notaentrada": {"chave": ["numeronota", "id_loja", "id_fornecedor"], "data": "dataentrada", "decimais": ["valortotal", "valormercadoria", "valordesconto"]},
    "notaentradaitem": {"chave": ["id"], "data": None, "decimais": ["quantidade", "valor", "valortotal"]},
    "perda": {"chave": ["id"], "data": "data", "decimais": ["quantidade", "valor"]},
    "logestoque": {"chave": ["id"], "data": "datamovimento", "decimais": ["quantidade", "estoqueanterior", "estoqueatual", "custocomimposto", "custosemimposto", "customediocomimposto", "customediosemimposto"]},
    "promocao": {"chave": ["id"], "data": "datainicio", "decimais": ["valor", "valordesconto"]},
    "promocaoitem": {"chave": ["id"], "data": None, "decimais": ["precovenda"]},
    "pedido": {"chave": ["id"], "data": "datacompra", "decimais": []},
    "pedidoitem": {"chave": ["id"], "data": None, "decimais": ["quantidade", "custocompra", "valortotal"]},
    "oferta": {"chave": ["id"], "data": "datainicio", "decimais": ["precooferta", "preconormal"], "try_decimais": ["precoimediato"]},
    "pagarfornecedor": {"chave": ["id"], "data": "dataemissao", "decimais": ["valor"]},
    "pagarfornecedorparcela": {"chave": ["id"], "data": "datavencimento", "decimais": ["valor", "valoracrescimo"], "datas_extras": ["datapagamento", "datapagamentocontabil"]},
    "pagaroutrasdespesas": {"chave": ["id"], "data": "dataemissao", "decimais": ["valor", "valorbruto"]},
    "pagaroutrasdespesasimposto": {"chave": ["id"], "data": "datavencimento", "decimais": ["valor", "basecalculo", "aliquota"]},
}

if ENTITY not in CONFIG:
    raise Exception(f"Entity inválida para D6B: {ENTITY}")


def latest_mature_partition(entity):
    source = f"{BRONZE_SOURCE_CATALOG}.bronze.{entity}"
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


def aplicar_casts(df, cfg):
    for col in cfg["decimais"]:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"),
            )

    for col in cfg.get("try_decimais", []):
        if col in df.columns:
            df = df.withColumn(
                col,
                F.expr(f"try_cast(replace(`{col}`, ',', '.') as decimal(14,3))"),
            )

    if cfg["data"] and cfg["data"] in df.columns:
        df = (
            df.withColumn(
                cfg["data"],
                F.to_timestamp(F.col(cfg["data"]), "yyyy/MM/dd HH:mm:ss.SSS"),
            )
            .withColumn("ano", F.year(cfg["data"]))
            .withColumn("mes", F.month(cfg["data"]))
        )

    for col in cfg.get("datas_extras", []):
        if col in df.columns:
            df = df.withColumn(
                col,
                F.expr(
                    f"try_to_timestamp(`{col}`, 'yyyy/MM/dd HH:mm:ss.SSS')"
                ),
            )
    return df


def filtrar_contrato(entity, df):
    path = f"{BUNDLE_FILES_PATH}/contracts/silver/{entity}.yaml"
    try:
        with open(path, "r") as f:
            contract = yaml.safe_load(f)
    except FileNotFoundError:
        return df, df.where(F.lit(False))

    work = df.withColumn("_invalido", F.lit(False))
    for cfg in contract.get("columns", []):
        name = cfg.get("name")
        if name not in work.columns:
            continue

        if not cfg.get("nullable", True):
            work = work.withColumn(
                "_invalido",
                F.when(F.col(name).isNull(), F.lit(True))
                 .otherwise(F.col("_invalido")),
            )

        min_val = cfg.get("min")
        if min_val is not None:
            try:
                min_num = float(min_val)
                work = work.withColumn(
                    "_invalido",
                    F.when(F.col(name).cast("double") < min_num, F.lit(True))
                     .otherwise(F.col("_invalido")),
                )
            except (TypeError, ValueError):
                pass

    return (
        work.where(~F.col("_invalido")).drop("_invalido"),
        work.where(F.col("_invalido")).drop("_invalido"),
    )


cfg = CONFIG[ENTITY]
keys = cfg["chave"]
bronze = f"{CATALOG}.bronze.{ENTITY}"
silver = f"{CATALOG}.silver.{ENTITY}"

state_rows = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity") == ENTITY)
    .collect()
)
if len(state_rows) != 1:
    raise Exception(f"{ENTITY}: watermark esperado=1; encontrado={len(state_rows)}")

state = state_rows[0]
committed_before = state["last_processed_snapshot"]
candidate_before = state["candidate_snapshot"]
status_before = state["status"]

mature_cutoff = latest_mature_partition(ENTITY)
if mature_cutoff is None:
    raise Exception(f"{ENTITY}: nenhuma partição madura encontrada")

silver_version_before = (
    spark.sql(f"DESCRIBE HISTORY {silver} LIMIT 1")
    .select("version")
    .collect()[0]["version"]
)
rows_before = spark.table(silver).count()
future_before = (
    spark.table(silver)
    .filter(F.col("ingestion_date") > F.lit(mature_cutoff))
    .count()
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        entity STRING,
        repair_started_at TIMESTAMP,
        silver_version_before BIGINT,
        watermark_committed_before DATE,
        watermark_candidate_before DATE,
        watermark_status_before STRING,
        mature_cutoff DATE,
        rows_before BIGINT,
        future_rows_before BIGINT
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {AUDIT_TABLE}
    VALUES (
        '{ENTITY}',
        current_timestamp(),
        {silver_version_before},
        {f"DATE '{committed_before}'" if committed_before else "NULL"},
        {f"DATE '{candidate_before}'" if candidate_before else "NULL"},
        '{status_before}',
        DATE '{mature_cutoff}',
        {rows_before},
        {future_before}
    )
""")

raw = spark.table(bronze).filter(
    F.col("ingestion_date") <= F.lit(mature_cutoff)
)
valid, invalid = filtrar_contrato(ENTITY, aplicar_casts(raw, cfg))

w = Window.partitionBy(*keys).orderBy(F.col("ingestion_date").desc())
expected = (
    valid.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

cond_merge = " AND ".join([f"t.{k} = s.{k}" for k in keys])
(
    DeltaTable.forName(spark, silver).alias("t")
    .merge(expected.alias("s"), cond_merge)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# Rollback somente do que jamais deveria ter entrado: linhas vindas de
# partição ainda aberta. Isso NÃO é business delete.
DeltaTable.forName(spark, silver).delete(
    F.col("ingestion_date") > F.lit(mature_cutoff)
)

spark.sql(f"""
    UPDATE {CONTROL_TABLE}
    SET candidate_snapshot = DATE '{mature_cutoff}',
        status = 'REPAIR_PENDING_VALIDATION',
        updated_at = current_timestamp()
    WHERE entity = '{ENTITY}'
""")

rows_after = spark.table(silver).count()
future_after = (
    spark.table(silver)
    .filter(F.col("ingestion_date") > F.lit(mature_cutoff))
    .count()
)

print(f"\n=== D6B REPAIR APPLY — {ENTITY} ===")
print(f"state before: committed={committed_before} candidate={candidate_before} status={status_before}")
print(f"mature_cutoff: {mature_cutoff}")
print(f"silver version before: {silver_version_before}")
print(f"rows: before={rows_before:,} after={rows_after:,}")
print(f"future rows: before={future_before:,} after={future_after:,}")
print(f"expected mature keys: {expected.count():,}")
print(f"invalid mature rows ignored by Silver contract: {invalid.count():,}")
print("✅ Mature baseline MERGE aplicado sem deletar histórico observado.")
print("✅ Linhas acima do mature_cutoff removidas como rollback de partição aberta.")
print("✅ Watermark ainda não committed; status=REPAIR_PENDING_VALIDATION.")
