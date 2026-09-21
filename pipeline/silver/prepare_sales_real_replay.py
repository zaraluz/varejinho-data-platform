# Databricks notebook source
# pipeline/silver/prepare_sales_real_replay.py
# Gate D7D — prepara sandbox com dados REAIS da última partição madura de venda.
#
# Estratégia:
# - descobre mature_cutoff real e a partição madura anterior
# - limita o sandbox aos ids presentes no mature_cutoff (pequeno e fiel)
# - Bronze sandbox contém o histórico desses ids até o cutoff
# - Silver sandbox começa no estado correto até a partição anterior
# - watermark sandbox começa na partição anterior

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

SOURCE = f"{BRONZE_SOURCE_CATALOG}.bronze.venda"
REAL_BRONZE = f"{CATALOG}.bronze.venda"
SB_BRONZE = f"{CATALOG}.control._d7d_venda_real_bronze"
SB_SILVER = f"{CATALOG}.silver._d7d_venda_real"
SB_CONTROL = f"{CATALOG}.control._d7d_venda_watermark"
SB_META = f"{CATALOG}.control._d7d_venda_meta"
SB_QUAR = f"{CATALOG}.silver._d7d_quarantine_venda"
SB_HIST = f"{CATALOG}.silver._d7d_quarantine_history_venda"
CONTRACT = f"{BUNDLE_FILES_PATH}/contracts/silver/venda.yaml"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D7D só pode executar em *_dev. Recebido: {CATALOG}")

for table in [SB_HIST, SB_QUAR, SB_SILVER, SB_CONTROL, SB_META, SB_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")


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

        min_val = cfg.get("min")
        if min_val is not None:
            try:
                min_num = float(min_val)
                work = work.withColumn(
                    "_invalido",
                    F.when(F.col(name).cast("double") < min_num, True)
                     .otherwise(F.col("_invalido")),
                )
            except (TypeError, ValueError):
                pass

    return work.where(~F.col("_invalido")).drop("_invalido")


maturity = spark.sql(f"""
    SELECT
        ingestion_date,
        MIN(to_date(_metadata.file_modification_time)) AS min_modified_date
    FROM {SOURCE}
    GROUP BY ingestion_date
""")

mature_dates = [
    r["ingestion_date"]
    for r in (
        maturity
        .filter(F.col("min_modified_date") > F.col("ingestion_date"))
        .select("ingestion_date")
        .distinct()
        .orderBy(F.col("ingestion_date").desc())
        .limit(2)
        .collect()
    )
]

if len(mature_dates) < 2:
    raise Exception("Gate D7D requer pelo menos duas partições maduras")

target = mature_dates[0]
previous = mature_dates[1]

target_ids = (
    spark.table(REAL_BRONZE)
    .filter(F.col("ingestion_date") == F.lit(target))
    .select("id")
    .distinct()
)

target_id_count = target_ids.count()
if target_id_count == 0:
    raise Exception(f"Nenhum id encontrado na partição madura {target}")

sandbox_bronze = (
    spark.table(REAL_BRONZE)
    .filter(F.col("ingestion_date") <= F.lit(target))
    .join(target_ids, on="id", how="inner")
)

sandbox_bronze.write.format("delta").mode("overwrite").saveAsTable(SB_BRONZE)

baseline = valid_only(
    transform(
        sandbox_bronze.filter(F.col("ingestion_date") <= F.lit(previous))
    )
)

w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
baseline = (
    baseline
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

baseline.write.format("delta").mode("overwrite").saveAsTable(SB_SILVER)

spark.sql(f"""
    CREATE TABLE {SB_CONTROL} (
        entity STRING NOT NULL,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING NOT NULL,
        updated_at TIMESTAMP NOT NULL
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {SB_CONTROL}
    VALUES ('venda', DATE '{previous}', NULL, 'COMMITTED', current_timestamp())
""")

spark.createDataFrame(
    [(str(previous), str(target), target_id_count)],
    "previous string, target string, target_ids long",
).write.format("delta").mode("overwrite").saveAsTable(SB_META)

print("\n=== GATE D7D — PREPARE REAL VENDA REPLAY ===")
print(f"previous committed: {previous}")
print(f"target mature:      {target}")
print(f"target ids:         {target_id_count:,}")
print(f"sandbox Bronze:     {spark.table(SB_BRONZE).count():,} rows")
print(f"sandbox Silver:     {spark.table(SB_SILVER).count():,} rows")
print("✅ Sandbox real criado sem alterar Silver real.")
