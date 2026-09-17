# Databricks notebook source
# pipeline/silver/validate_scd2_product.py
# Gate B4 — invariantes do SCD2 de produto reconstruído no ambiente dev.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = f"{CATALOG}.bronze.produto"
SILVER = f"{CATALOG}.silver.produto"
KEY = "id"
SNAPSHOT = "ingestion_date"

TYPE2_COLS = [
    "descricaocompleta",
    "mercadologico1",
    "mercadologico2",
    "mercadologico3",
    "ncm1",
    "id_tipoembalagem",
]
TYPE1_COLS = [
    "descricaoreduzida",
    "id_tipomercadoria",
    "pesoliquido",
    "pesobruto",
]

if not CATALOG.endswith("_dev"):
    raise Exception(f"Proteção de hardening: validação esperava catálogo *_dev. Recebido: {CATALOG}")

print("\n=== GATE B4 — PRODUCT SCD2 QUALITY GATE ===")
print(f"Bronze: {BRONZE}")
print(f"Silver: {SILVER}\n")

bronze = spark.table(BRONZE)
silver = spark.table(SILVER)
checks = []


def check(name: str, failures: int, detail: str = ""):
    ok = failures == 0
    prefix = "✅" if ok else "❌"
    msg = f"{prefix} {name}"
    if detail:
        msg += f" — {detail}"
    checks.append((ok, msg))
    print(msg)


# Hash definitivo do modeling.
hash_expr = F.md5(
    F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TYPE2_COLS],
    )
)

# 1) Reconstrói de forma independente quantos change points a Bronze implica.
w_bronze = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
expected = (
    bronze.withColumn("_expected_hash", hash_expr)
          .withColumn("_prev_hash", F.lag("_expected_hash").over(w_bronze))
          .filter(F.col("_prev_hash").isNull() | (F.col("_expected_hash") != F.col("_prev_hash")))
)
expected_rows = expected.count()
actual_rows = silver.count()
check(
    "Cardinalidade = change points da Bronze",
    abs(actual_rows - expected_rows),
    f"expected={expected_rows:,} | actual={actual_rows:,}",
)

expected_ids = bronze.select(KEY).distinct().count()
actual_ids = silver.select(KEY).distinct().count()
check(
    "Todos os produtos preservados",
    abs(actual_ids - expected_ids),
    f"Bronze ids={expected_ids:,} | Silver ids={actual_ids:,}",
)

# 2) Grain e corrente única.
dup_grain = (
    silver.groupBy(KEY, "valid_from").count().filter(F.col("count") > 1).count()
)
check("Grain única (id, valid_from)", dup_grain, f"duplicatas={dup_grain:,}")

current_per_id = (
    silver.groupBy(KEY)
          .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_count"))
)
bad_current = current_per_id.filter(F.col("current_count") != 1).count()
check("Exatamente 1 versão current por produto", bad_current, f"ids inválidos={bad_current:,}")

bad_current_valid_to = silver.filter(F.col("is_current") & F.col("valid_to").isNotNull()).count()
check("Versão current tem valid_to NULL", bad_current_valid_to, f"linhas inválidas={bad_current_valid_to:,}")

bad_closed_valid_to = silver.filter((~F.col("is_current")) & F.col("valid_to").isNull()).count()
check("Versão histórica tem valid_to preenchido", bad_closed_valid_to, f"linhas inválidas={bad_closed_valid_to:,}")

# 3) Intervalos contíguos e sem sobreposição.
w_silver = Window.partitionBy(KEY).orderBy(F.col("valid_from").asc())
intervals = silver.withColumn("_next_valid_from", F.lead("valid_from").over(w_silver))

bad_order = silver.filter(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).count()
check("valid_from < valid_to em versões fechadas", bad_order, f"linhas inválidas={bad_order:,}")

bad_continuity = intervals.filter(~F.col("valid_to").eqNullSafe(F.col("_next_valid_from"))).count()
check("valid_to = próximo valid_from", bad_continuity, f"intervalos quebrados={bad_continuity:,}")

# 4) Hash armazenado representa exatamente os atributos Type 2 definidos.
bad_hash = silver.withColumn("_recomputed_hash", hash_expr).filter(
    F.col("hash_versao") != F.col("_recomputed_hash")
).count()
check("hash_versao íntegro", bad_hash, f"linhas inválidas={bad_hash:,}")

same_hash_consecutive = (
    silver.withColumn("_prev_hash", F.lag("hash_versao").over(w_silver))
          .filter(F.col("_prev_hash").isNotNull() & (F.col("hash_versao") == F.col("_prev_hash")))
          .count()
)
check("Nenhuma versão redundante com mesmo hash consecutivo", same_hash_consecutive, f"linhas redundantes={same_hash_consecutive:,}")

# 5) Type 1: todas as versões devem refletir o último valor conhecido do produto.
w_latest = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).desc())
latest = (
    bronze.withColumn("_rn", F.row_number().over(w_latest))
          .filter(F.col("_rn") == 1)
          .select(KEY, *[F.col(c).alias(f"_latest_{c}") for c in TYPE1_COLS])
)
joined_t1 = silver.join(latest, on=KEY, how="left")
for c in TYPE1_COLS:
    mismatches = joined_t1.filter(~F.col(c).eqNullSafe(F.col(f"_latest_{c}"))).count()
    check(f"Type 1 atualizado em todas as versões: {c}", mismatches, f"mismatches={mismatches:,}")

# 6) A versão current precisa corresponder ao estado Type 2 do último snapshot Bronze.
latest_type2 = (
    bronze.withColumn("_rn", F.row_number().over(w_latest))
          .filter(F.col("_rn") == 1)
          .select(KEY, *[F.col(c).alias(f"_latest_t2_{c}") for c in TYPE2_COLS])
)
current = silver.filter(F.col("is_current")).join(latest_type2, on=KEY, how="left")
for c in TYPE2_COLS:
    mismatches = current.filter(~F.col(c).eqNullSafe(F.col(f"_latest_t2_{c}"))).count()
    check(f"Current reflete último snapshot Type 2: {c}", mismatches, f"mismatches={mismatches:,}")

# 7) Observabilidade temporal.
invalid_source = silver.filter(~F.col("valid_from_source").isin("datacadastro", "dataalteracao", "ingestion_date")).count()
check("valid_from_source usa origem conhecida", invalid_source, f"linhas inválidas={invalid_source:,}")

source_counts = {
    r["valid_from_source"]: r["count"]
    for r in silver.groupBy("valid_from_source").count().collect()
}
versioned_ids = silver.groupBy(KEY).count().filter(F.col("count") > 1).count()
max_versions = silver.groupBy(KEY).count().agg(F.max("count")).collect()[0][0]

print("\n--- RESUMO SCD2 ---")
print(f"versões totais:          {actual_rows:,}")
print(f"produtos distintos:      {actual_ids:,}")
print(f"ids com >1 versão:       {versioned_ids:,}")
print(f"máximo versões / id:     {max_versions}")
print(f"origem de valid_from:    {source_counts}")

# Exemplos reais para inspeção humana.
print("\nExemplos de produtos versionados (até 10 ids):")
example_ids = [r[KEY] for r in silver.groupBy(KEY).count().filter(F.col("count") > 1).orderBy(KEY).limit(10).collect()]
if example_ids:
    for r in (
        silver.filter(F.col(KEY).isin(example_ids))
              .select(KEY, "descricaocompleta", "mercadologico1", "mercadologico2", "mercadologico3", "ncm1", "id_tipoembalagem", "valid_from", "valid_to", "is_current", "valid_from_source")
              .orderBy(KEY, "valid_from")
              .collect()
    ):
        print(
            f"  id={r[KEY]} | {r['valid_from']} -> {r['valid_to']} | current={r['is_current']} | "
            f"source={r['valid_from_source']} | desc={repr(r['descricaocompleta'])} | "
            f"merc={r['mercadologico1']}/{r['mercadologico2']}/{r['mercadologico3']} | embalagem={r['id_tipoembalagem']}"
        )

failed = [msg for ok, msg in checks if not ok]
print(f"\n=== RESULTADO: {len(checks) - len(failed)}/{len(checks)} checks passaram ===")
if failed:
    raise Exception("Gate B4 falhou:\n" + "\n".join(failed))

print("✅ Produto SCD2 aprovado no ambiente dev. Ainda NÃO integrado ao pipeline diário/prod.")
