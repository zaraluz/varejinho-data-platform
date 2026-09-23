# Databricks notebook source
# validation/sales/verify_sales_real_replay.py
# Gate D7D — compara o resultado incremental sandbox com o expected real.

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

SB_BRONZE = f"{CATALOG}.control._d7d_venda_real_bronze"
SB_SILVER = f"{CATALOG}.silver._d7d_venda_real"
SB_CONTROL = f"{CATALOG}.control._d7d_venda_watermark"
SB_META = f"{CATALOG}.control._d7d_venda_meta"
SB_QUAR = f"{CATALOG}.silver._d7d_quarantine_venda"
SB_HIST = f"{CATALOG}.silver._d7d_quarantine_history_venda"
CONTRACT = f"{BUNDLE_FILES_PATH}/contracts/silver/venda.yaml"


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


meta = spark.table(SB_META).collect()[0]
target = meta["target"]
target_date = F.to_date(F.lit(target))

expected = valid_only(
    transform(
        spark.table(SB_BRONZE)
        .filter(F.col("ingestion_date") <= target_date)
    )
)

w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
expected = (
    expected
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

actual = spark.table(SB_SILVER)

e_schema = {f.name: f.dataType.simpleString() for f in expected.schema.fields}
a_schema = {f.name: f.dataType.simpleString() for f in actual.schema.fields}
schema_ok = e_schema == a_schema

e_rows = expected.count()
a_rows = actual.count()
missing = expected.select("id").join(
    actual.select("id"), on="id", how="left_anti"
).count()
extra = actual.select("id").join(
    expected.select("id"), on="id", how="left_anti"
).count()

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

state = (
    spark.table(SB_CONTROL)
    .filter(F.col("entity") == "venda")
    .collect()[0]
)

ok = (
    schema_ok
    and e_rows == a_rows
    and missing == 0
    and extra == 0
    and mismatches == 0
    and str(state["last_processed_snapshot"]) == target
    and state["candidate_snapshot"] is None
    and state["status"] == "COMMITTED"
)

print("\n=== RESULTADO D7D — REAL VENDA REPLAY ===")
print(f"target mature:   {target}")
print(f"rows:            expected={e_rows:,} | actual={a_rows:,}")
print(f"schema exact:    {schema_ok}")
print(f"missing:         {missing:,}")
print(f"extra:           {extra:,}")
print(f"value mismatch:  {mismatches:,}")
print(
    f"watermark:       committed={state['last_processed_snapshot']} | "
    f"candidate={state['candidate_snapshot']} | status={state['status']}"
)

if not ok:
    raise Exception("Gate D7D falhou; sandbox preservado para diagnóstico.")

for table in [SB_HIST, SB_QUAR, SB_SILVER, SB_CONTROL, SB_META, SB_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("✅ Incremental real de venda == expected real.")
print("✅ Replay após commit permaneceu no-op.")
print("✅ Sandbox real removido após sucesso.")
