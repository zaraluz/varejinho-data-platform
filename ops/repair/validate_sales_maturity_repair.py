# Databricks notebook source
# ops/repair/validate_sales_maturity_repair.py
# Gate D7B — prova equivalência exata da venda madura antes do seed/commit.

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
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)

BRONZE = f"{CATALOG}.bronze.venda"
SILVER = f"{CATALOG}.silver.venda"
CONTROL = f"{CATALOG}.control.fact_watermark"
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


state = (
    spark.table(CONTROL)
    .filter(F.col("entity") == "venda")
    .collect()
)
if len(state) != 1:
    raise Exception(f"venda: watermark esperado=1; encontrado={len(state)}")

row = state[0]
candidate = row["candidate_snapshot"]
status = row["status"]

if status != "REPAIR_PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"venda: estado inválido para repair validation: "
        f"candidate={candidate} status={status}"
    )

expected = (
    spark.table(BRONZE)
    .filter(F.col("ingestion_date") <= F.lit(candidate))
)
expected = valid_only(transform(expected))

w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
expected = (
    expected.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

actual = spark.table(SILVER)

e_schema = {f.name: f.dataType.simpleString() for f in expected.schema.fields}
a_schema = {f.name: f.dataType.simpleString() for f in actual.schema.fields}
schema_ok = e_schema == a_schema

e_rows = expected.count()
a_rows = actual.count()
e_dup = expected.groupBy("id").count().filter("count > 1").count()
a_dup = actual.groupBy("id").count().filter("count > 1").count()

e_keys = expected.select("id")
a_keys = actual.select("id")
missing = e_keys.join(a_keys, on="id", how="left_anti").count()
extra = a_keys.join(e_keys, on="id", how="left_anti").count()
future = actual.filter(F.col("ingestion_date") > F.lit(candidate)).count()

mismatches = None
if schema_ok and e_dup == 0 and a_dup == 0:
    cols = expected.columns
    nonkeys = [c for c in cols if c != "id"]
    e = expected.alias("e")
    a = actual.alias("a")
    joined = e.join(a, F.col("e.id").eqNullSafe(F.col("a.id")), "inner")

    diff = reduce(
        lambda acc, c: acc | (~F.col(f"e.{c}").eqNullSafe(F.col(f"a.{c}"))),
        nonkeys[1:],
        ~F.col(f"e.{nonkeys[0]}").eqNullSafe(F.col(f"a.{nonkeys[0]}")),
    )
    mismatches = joined.filter(diff).count()

ok = (
    schema_ok
    and e_rows == a_rows
    and e_dup == 0
    and a_dup == 0
    and missing == 0
    and extra == 0
    and future == 0
    and mismatches == 0
)

print("\n=== D7B SALES MATURITY REPAIR — VALIDATE ===")
print(f"candidate/mature cutoff: {candidate}")
print(f"schema exact:             {schema_ok}")
print(f"rows: expected={e_rows:,} | actual={a_rows:,}")
print(f"duplicate ids: expected={e_dup:,} | actual={a_dup:,}")
print(f"missing keys:             {missing:,}")
print(f"extra keys:               {extra:,}")
print(f"rows > candidate:         {future:,}")
print(f"value mismatches:         {mismatches}")
print(f"RESULT:                   {'✅ PASS' if ok else '❌ FAIL'}")

if not ok:
    raise Exception("venda: repair não reproduziu exatamente o baseline maduro")

print("✅ Silver venda == full mature rebuild.")
