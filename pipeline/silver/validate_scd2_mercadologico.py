# Databricks notebook source
# pipeline/silver/validate_scd2_mercadologico.py
# Gate B8 — invariantes do SCD2 mercadológico.

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
    raise Exception(f"validate_scd2_mercadologico só pode executar em *_dev. Recebido: {CATALOG}")

bronze = spark.table(BRONZE)
silver = spark.table(SILVER)
checks = []

def check(name, failures, detail=""):
    ok = failures == 0
    print(("✅" if ok else "❌") + f" {name}" + (f" — {detail}" if detail else ""))
    checks.append(ok)

hash_expr = F.md5(F.concat_ws(
    "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TYPE2_COLS]
))
w_bronze = Window.partitionBy(KEY).orderBy(SNAPSHOT)
expected = (
    bronze.withColumn("_hash", hash_expr)
          .withColumn("_prev", F.lag("_hash").over(w_bronze))
          .filter(F.col("_prev").isNull() | (F.col("_hash") != F.col("_prev")))
)

check("Cardinalidade = change points Type 2",
      abs(expected.count()-silver.count()),
      f"expected={expected.count():,} | actual={silver.count():,}")
check("Todos os ids preservados",
      abs(bronze.select(KEY).distinct().count()-silver.select(KEY).distinct().count()))
check("Grain única (id, valid_from)",
      silver.groupBy(KEY,"valid_from").count().filter(F.col("count")>1).count())

bad_current = (
    silver.groupBy(KEY)
          .agg(F.sum(F.when(F.col("is_current"),1).otherwise(0)).alias("n"))
          .filter(F.col("n") != 1).count()
)
check("Exatamente 1 current por id", bad_current)
check("Current tem valid_to NULL",
      silver.filter(F.col("is_current") & F.col("valid_to").isNotNull()).count())
check("Histórico tem valid_to preenchido",
      silver.filter((~F.col("is_current")) & F.col("valid_to").isNull()).count())

w_silver = Window.partitionBy(KEY).orderBy("valid_from")
intervals = silver.withColumn("_next", F.lead("valid_from").over(w_silver))
check("valid_from < valid_to",
      silver.filter(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).count())
check("valid_to = próximo valid_from",
      intervals.filter(~F.col("valid_to").eqNullSafe(F.col("_next"))).count())
check("hash_versao íntegro",
      silver.withColumn("_re",hash_expr).filter(F.col("hash_versao") != F.col("_re")).count())
check("Sem versões redundantes consecutivas",
      silver.withColumn("_prev",F.lag("hash_versao").over(w_silver))
            .filter(F.col("_prev").isNotNull() & (F.col("_prev")==F.col("hash_versao"))).count())

w_latest = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).desc())
latest = bronze.withColumn("_rn",F.row_number().over(w_latest)).filter(F.col("_rn")==1)
joined = silver.join(latest.select(KEY,F.col("descricao").alias("_latest_desc")),KEY,"left")
check("Type 1 descricao = último snapshot em todas as versões",
      joined.filter(~F.col("descricao").eqNullSafe(F.col("_latest_desc"))).count())

latest_t2 = latest.select(KEY,*[F.col(c).alias(f"_latest_{c}") for c in TYPE2_COLS])
current = silver.filter("is_current").join(latest_t2,KEY,"left")
for c in TYPE2_COLS:
    check(f"Current reflete último snapshot: {c}",
          current.filter(~F.col(c).eqNullSafe(F.col(f"_latest_{c}"))).count())

check("valid_from_source = ingestion_date",
      silver.filter(F.col("valid_from_source") != "ingestion_date").count())

first_expected = (
    bronze.groupBy(KEY).agg(F.min(SNAPSHOT).alias("_first_snapshot"))
)
first_actual = (
    silver.withColumn("_rn",F.row_number().over(w_silver)).filter(F.col("_rn")==1)
          .select(KEY,"valid_from")
)
first_join = first_actual.join(first_expected,KEY,"left")
check("Primeira versão = primeiro snapshot observado",
      first_join.filter(F.to_date("valid_from") != F.col("_first_snapshot")).count())

failed = len([x for x in checks if not x])
print(f"\n=== RESULTADO: {len(checks)-failed}/{len(checks)} checks passaram ===")
if failed:
    raise Exception(f"Gate mercadologico falhou em {failed} check(s)")
print("✅ Mercadologico SCD2 aprovado na tabela validada.")
