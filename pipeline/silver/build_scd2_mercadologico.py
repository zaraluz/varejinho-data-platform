# Databricks notebook source
# pipeline/silver/build_scd2_mercadologico.py
# Gate B8B — backfill determinístico do SCD2 mercadológico.
# Sem timestamp de ERP: valid_from = primeiro snapshot em que o estado foi observado.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default

CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.mercadologico")
SILVER = job_param("silver_table", f"{CATALOG}.silver.mercadologico")
KEY = "id"
SNAPSHOT = "ingestion_date"

TYPE2_COLS = [
    "mercadologico1","mercadologico2","mercadologico3",
    "mercadologico4","mercadologico5","nivel",
]
TYPE1_COLS = ["descricao"]

if not CATALOG.endswith("_dev"):
    raise Exception(f"build_scd2_mercadologico só pode executar em *_dev. Recebido: {CATALOG}")

raw = spark.table(BRONZE)
required = {KEY, SNAPSHOT, *TYPE2_COLS, *TYPE1_COLS}
missing = sorted(required - set(raw.columns))
if missing:
    raise Exception(f"Colunas obrigatórias ausentes em {BRONZE}: {missing}")

if raw.filter(F.col(KEY).isNull()).count():
    raise Exception("Natural key nula em mercadologico")

dup = raw.groupBy(KEY, SNAPSHOT).count().filter(F.col("count") > 1).count()
if dup:
    raise Exception(f"Grain inválido: {dup:,} duplicata(s) por ({KEY}, {SNAPSHOT})")

hash_expr = F.md5(F.concat_ws(
    "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TYPE2_COLS]
))

base = raw.withColumn("hash_versao", hash_expr)
w_hist = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
change_points = (
    base.withColumn("_prev_hash", F.lag("hash_versao").over(w_hist))
        .filter(F.col("_prev_hash").isNull() | (F.col("hash_versao") != F.col("_prev_hash")))
        .withColumn("valid_from", F.col(SNAPSHOT).cast("timestamp"))
        .withColumn("valid_from_source", F.lit("ingestion_date"))
)

# Type 1: descrição atual propagada para todas as versões.
w_latest = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).desc())
latest_t1 = (
    raw.withColumn("_rn", F.row_number().over(w_latest))
       .filter(F.col("_rn") == 1)
       .select(KEY, *[F.col(c).alias(f"_type1_{c}") for c in TYPE1_COLS])
)

keep = [c for c in change_points.columns if c not in TYPE1_COLS]
versions = change_points.select(*keep).join(latest_t1, on=KEY, how="left")
for c in TYPE1_COLS:
    versions = versions.withColumn(c, F.col(f"_type1_{c}")).drop(f"_type1_{c}")

w_versions = Window.partitionBy(KEY).orderBy("valid_from")
versions = (
    versions.withColumn("valid_to", F.lead("valid_from").over(w_versions))
            .withColumn("is_current", F.col("valid_to").isNull())
            .withColumn("scd_source_snapshot", F.col(SNAPSHOT).cast("date"))
            .select(
                KEY, *TYPE2_COLS, *TYPE1_COLS,
                "hash_versao","valid_from","valid_to","is_current",
                "scd_source_snapshot","valid_from_source"
            )
)

(versions.write.format("delta").mode("overwrite")
         .option("overwriteSchema","true").saveAsTable(SILVER))

out = spark.table(SILVER)
print("\n=== GATE B8B — BUILD MERCADOLOGICO SCD2 ===")
print(f"Fonte:   {BRONZE}")
print(f"Destino: {SILVER}")
print(f"versões totais:      {out.count():,}")
print(f"ids distintos:       {out.select(KEY).distinct().count():,}")
print(f"ids com >1 versão:   {out.groupBy(KEY).count().filter(F.col('count')>1).count():,}")
print("Política temporal: valid_from = primeiro ingestion_date em que cada estado foi observado.")
