# Databricks notebook source
# pipeline/silver/repair_sales_maturity_baseline.py
# Gate D7B — one-time repair da Silver venda após provar maturidade D+1.
#
# O transform_sales legado já consumiu a partição aberta. Este repair:
# - calcula o mature_cutoff pela external Bronze física
# - restaura, para ids sobrescritos por partição aberta, a última versão madura
# - remove open-only inserts (se existirem)
# - grava candidate em estado REPAIR_PENDING_VALIDATION
# - NÃO commita watermark antes da validação

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
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)

BRONZE = f"{CATALOG}.bronze.venda"
SILVER = f"{CATALOG}.silver.venda"
CONTROL = f"{CATALOG}.control.fact_watermark"
AUDIT = f"{CATALOG}.control.sales_maturity_repair_audit"
CONTRACT = f"{BUNDLE_FILES_PATH}/contracts/silver/venda.yaml"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D7B só pode executar em *_dev. Recebido: {CATALOG}")


def transform(df):
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


def valid_only(df):
    with open(CONTRACT, "r") as f:
        contract = yaml.safe_load(f)

    work = df.withColumn("_invalido", F.lit(False))

    for cfg in contract.get("columns", []):
        name = cfg.get("name")
        if name not in work.columns:
            continue

        if not cfg.get("nullable", True):
            work = work.withColumn(
                "_invalido",
                F.when(F.col(name).isNull(), True).otherwise(F.col("_invalido")),
            )

        if cfg.get("min") is not None:
            try:
                min_num = float(cfg["min"])
                work = work.withColumn(
                    "_invalido",
                    F.when(F.col(name).cast("double") < min_num, True)
                     .otherwise(F.col("_invalido")),
                )
            except (TypeError, ValueError):
                pass

    return work.where(~F.col("_invalido")).drop("_invalido")


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

if mature_cutoff is None:
    raise Exception("venda: nenhuma partição madura encontrada")

history = spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").collect()[0]
version_before = history["version"]

silver_before = spark.table(SILVER)
rows_before = silver_before.count()
future_before = silver_before.filter(
    F.col("ingestion_date") > F.lit(mature_cutoff)
)
future_count = future_before.count()
future_ids = future_before.select("id").distinct()

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT} (
        repair_started_at TIMESTAMP,
        silver_version_before BIGINT,
        mature_cutoff DATE,
        rows_before BIGINT,
        future_rows_before BIGINT
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {AUDIT}
    VALUES (
        current_timestamp(),
        {version_before},
        DATE '{mature_cutoff}',
        {rows_before},
        {future_count}
    )
""")

# Última versão madura apenas dos ids contaminados pela partição aberta.
mature_for_future_ids = (
    spark.table(BRONZE)
    .filter(F.col("ingestion_date") <= F.lit(mature_cutoff))
    .join(future_ids, on="id", how="inner")
)

mature_for_future_ids = valid_only(transform(mature_for_future_ids))

w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
restore = (
    mature_for_future_ids
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

restore_count = restore.count()
open_only_count = future_ids.join(
    restore.select("id"), on="id", how="left_anti"
).count()

if restore_count:
    (
        DeltaTable.forName(spark, SILVER).alias("t")
        .merge(restore.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .execute()
    )

# Qualquer linha ainda acima do cutoff é open-only e deve sair até a partição fechar.
DeltaTable.forName(spark, SILVER).delete(
    F.col("ingestion_date") > F.lit(mature_cutoff)
)

existing = (
    spark.table(CONTROL)
    .filter(F.col("entity") == "venda")
    .collect()
)

if len(existing) > 1:
    raise Exception(f"venda: mais de um watermark existente ({len(existing)})")

if len(existing) == 0:
    spark.sql(f"""
        INSERT INTO {CONTROL}
        VALUES (
            'venda',
            NULL,
            DATE '{mature_cutoff}',
            'REPAIR_PENDING_VALIDATION',
            current_timestamp()
        )
    """)
else:
    spark.sql(f"""
        UPDATE {CONTROL}
        SET candidate_snapshot = DATE '{mature_cutoff}',
            status = 'REPAIR_PENDING_VALIDATION',
            updated_at = current_timestamp()
        WHERE entity = 'venda'
    """)

future_after = (
    spark.table(SILVER)
    .filter(F.col("ingestion_date") > F.lit(mature_cutoff))
    .count()
)

print("\n=== D7B SALES MATURITY REPAIR — APPLY ===")
print(f"mature_cutoff:                  {mature_cutoff}")
print(f"silver version before:          {version_before}")
print(f"Silver rows before:             {rows_before:,}")
print(f"open rows before:               {future_count:,}")
print(f"ids restored to mature version: {restore_count:,}")
print(f"open-only ids removed:           {open_only_count:,}")
print(f"open rows after:                {future_after:,}")
print("✅ Repair aplicado; watermark ainda NÃO committed.")
print("✅ status=REPAIR_PENDING_VALIDATION.")
