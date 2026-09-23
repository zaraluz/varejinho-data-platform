# Databricks notebook source
# validation/sales/profile_sales_incrementality.py
# Gate D7A — diagnóstico read-only da venda antes de migrar transform_sales.py.
#
# Responde:
# - o grão (id, ingestion_date) é íntegro?
# - venda também segue maturidade D+1?
# - qual é o mature_cutoff atual?
# - a Silver contém linhas da partição ainda aberta?
# - quantas linhas abertas são inserts novos vs updates de ids já maduros?
# - o estado maduro esperado continua reproduzível?

from functools import reduce
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

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D7A só pode executar em *_dev. Recebido: {CATALOG}")

SOURCE = f"{BRONZE_SOURCE_CATALOG}.bronze.venda"
BRONZE = f"{CATALOG}.bronze.venda"
SILVER = f"{CATALOG}.silver.venda"
CONTRACT = f"{BUNDLE_FILES_PATH}/contracts/silver/venda.yaml"
KEY = ["id"]


def aplicar_transformacao_venda(df):
    return (
        df
        .withColumn(
            "valortotal",
            F.regexp_replace(F.col("valortotal"), ",", ".").cast("decimal(14,2)"),
        )
        .withColumn(
            "quantidade",
            F.regexp_replace(F.col("quantidade"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "custocomimposto",
            F.regexp_replace(F.col("custocomimposto"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "custosemimposto",
            F.regexp_replace(F.col("custosemimposto"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "customediocomimposto",
            F.regexp_replace(F.col("customediocomimposto"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "customediosemimposto",
            F.regexp_replace(F.col("customediosemimposto"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "piscofins",
            F.regexp_replace(F.col("piscofins"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "piscofinscredito",
            F.regexp_replace(F.col("piscofinscredito"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "icmscredito",
            F.regexp_replace(F.col("icmscredito"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "icmsdebito",
            F.regexp_replace(F.col("icmsdebito"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "precovenda",
            F.regexp_replace(F.col("precovenda"), ",", ".").cast("decimal(14,3)"),
        )
        .withColumn(
            "data",
            F.to_timestamp(F.col("data"), "yyyy/MM/dd HH:mm:ss.SSS"),
        )
        .withColumn("ano", F.year("data"))
        .withColumn("mes", F.month("data"))
        .withColumnRenamed("valortotal", "valor_total")
    )


def filtrar_contrato(df):
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

    return work.where(~F.col("_invalido")).drop("_invalido")


print("\n=== GATE D7A — VENDA INCREMENTALITY PROFILE ===")
print(f"source external: {SOURCE}")
print(f"dev Bronze:      {BRONZE}")
print(f"dev Silver:      {SILVER}")
print("Read-only: nenhum dado ou watermark será alterado.\n")

raw = spark.table(BRONZE)

# 1) Integridade de grão/snapshot.
total_rows = raw.count()
snapshots = raw.select("ingestion_date").distinct().count()
min_snapshot = raw.agg(F.min("ingestion_date")).collect()[0][0]
max_snapshot = raw.agg(F.max("ingestion_date")).collect()[0][0]
null_keys = raw.filter(F.col("id").isNull()).count()
dup_key_snapshot = (
    raw.groupBy("id", "ingestion_date")
       .count()
       .filter(F.col("count") > 1)
       .count()
)

print("--- GRAIN / SNAPSHOTS ---")
print(f"Bronze rows:                   {total_rows:,}")
print(f"Snapshots:                     {snapshots} | {min_snapshot} -> {max_snapshot}")
print(f"Null id rows:                  {null_keys:,}")
print(f"Duplicate id/snapshot groups:  {dup_key_snapshot:,}")

# 2) Maturidade física.
files = spark.sql(f"""
    SELECT
        ingestion_date,
        _metadata.file_path AS file_path,
        _metadata.file_modification_time AS file_modified_at
    FROM {SOURCE}
""").groupBy(
    "ingestion_date", "file_path", "file_modified_at"
).agg(
    F.count("*").alias("rows")
).withColumn(
    "modified_date", F.to_date("file_modified_at")
).withColumn(
    "lag_days", F.datediff(F.col("modified_date"), F.col("ingestion_date"))
)

maturity_stats = files.agg(
    F.min("lag_days").alias("min_lag"),
    F.expr("percentile_approx(lag_days, 0.5)").alias("median_lag"),
    F.max("lag_days").alias("max_lag"),
    F.sum(F.when(F.col("lag_days") > 1, 1).otherwise(0)).alias("files_gt_d1"),
).collect()[0]

mature_cutoff = (
    files.groupBy("ingestion_date")
    .agg(F.min("modified_date").alias("min_modified_date"))
    .filter(F.col("min_modified_date") > F.col("ingestion_date"))
    .agg(F.max("ingestion_date").alias("mature_cutoff"))
    .collect()[0]["mature_cutoff"]
)

print("\n--- PARTITION MATURITY ---")
print(
    f"Lag days: min={maturity_stats['min_lag']} | "
    f"median={maturity_stats['median_lag']} | "
    f"max={maturity_stats['max_lag']} | "
    f"files > D+1={maturity_stats['files_gt_d1']}"
)
print(f"Bronze max visível: {max_snapshot}")
print(f"Mature cutoff:      {mature_cutoff}")

print("Latest files:")
for row in files.orderBy(
    F.col("ingestion_date").desc(),
    F.col("file_modified_at").desc(),
).limit(5).collect():
    print(
        f"  ingestion_date={row['ingestion_date']} | "
        f"modified={row['file_modified_at']} | lag={row['lag_days']} | "
        f"rows={row['rows']:,}"
    )

# 3) Reconstrói estado maduro esperado usando exatamente a transformação atual.
mature_raw = raw.filter(F.col("ingestion_date") <= F.lit(mature_cutoff))
expected = filtrar_contrato(aplicar_transformacao_venda(mature_raw))

w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
expected = (
    expected.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

actual = spark.table(SILVER)
future = actual.filter(F.col("ingestion_date") > F.lit(mature_cutoff))

future_count = future.count()
future_keys = future.select("id").distinct()
expected_keys = expected.select("id").distinct()

future_updates = future_keys.join(
    expected_keys, on="id", how="inner"
).count()
future_inserts = future_keys.join(
    expected_keys, on="id", how="left_anti"
).count()

print("\n--- OPEN-PARTITION CONTAMINATION IN SILVER ---")
print(f"Silver total rows:              {actual.count():,}")
print(f"Rows above mature_cutoff:        {future_count:,}")
print(f"Open keys updating mature ids:   {future_updates:,}")
print(f"Open keys that are new inserts:  {future_inserts:,}")

# 4) Compara expected maduro com Silver para chaves que não foram sobrescritas
# pela partição aberta; isso separa problema histórico de mera contaminação D.
actual_nonfuture = actual.filter(
    F.col("ingestion_date") <= F.lit(mature_cutoff)
)

e_keys = expected.select("id")
a_keys = actual_nonfuture.select("id")

missing_nonfuture = e_keys.join(a_keys, on="id", how="left_anti")
extra_nonfuture = a_keys.join(e_keys, on="id", how="left_anti")

print("\n--- MATURE BASELINE COVERAGE ---")
print(f"Expected mature rows:            {expected.count():,}")
print(f"Silver rows <= mature_cutoff:    {actual_nonfuture.count():,}")
print(f"Missing mature keys:             {missing_nonfuture.count():,}")
print(f"Historical extra keys:           {extra_nonfuture.count():,}")

if missing_nonfuture.count():
    print("Amostra de mature keys ausentes:")
    missing_nonfuture.limit(10).show(truncate=False)

# 5) Mudanças de payload históricas por id, para confirmar necessidade de MERGE.
if dup_key_snapshot == 0:
    ignored = {"id", "ingestion_date"}
    payload_cols = sorted([c for c in raw.columns if c not in ignored])
    hash_expr = F.xxhash64(
        *[
            F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
            for c in payload_cols
        ]
    )

    hist = raw.select(
        "id",
        "ingestion_date",
        hash_expr.alias("_payload_hash"),
    )
    wh = Window.partitionBy("id").orderBy(F.col("ingestion_date"))
    hist = (
        hist.withColumn("_prev_hash", F.lag("_payload_hash").over(wh))
            .withColumn("_prev_snapshot", F.lag("ingestion_date").over(wh))
    )

    changes = hist.filter(
        F.col("_prev_snapshot").isNotNull()
        & (F.col("_payload_hash") != F.col("_prev_hash"))
    )

    print("\n--- HISTORICAL PAYLOAD CHURN ---")
    print(f"Change events:                  {changes.count():,}")
    print(f"Changed ids:                    {changes.select('id').distinct().count():,}")

print("\n=== D7A DECISION SIGNALS ===")
print(
    f"grain_ok={null_keys == 0 and dup_key_snapshot == 0} | "
    f"d_plus_1_compatible={(maturity_stats['files_gt_d1'] or 0) == 0} | "
    f"mature_cutoff={mature_cutoff} | "
    f"open_rows_in_silver={future_count}"
)

print("\n✅ D7A concluído em modo read-only.")
