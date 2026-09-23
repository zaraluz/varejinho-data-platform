# Databricks notebook source
# pipeline/silver/validate_scd2_supplier.py
# Gate B7 — invariantes do SCD2 de fornecedor.
# Pode validar a Silver real ou uma tabela sandbox parametrizada.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


def required_param(nome: str) -> str:
    """Parâmetro obrigatório do job: falha cedo em vez de cair num default de ambiente."""
    try:
        value = dbutils.widgets.get(nome)
    except Exception:
        value = ""
    if not value:
        raise ValueError(
            f"Parâmetro obrigatório ausente: '{nome}'. Execute via job do bundle, "
            "que injeta catalog/bundle_files_path/control_root/bronze_source_catalog por target."
        )
    return value


CATALOG = required_param("catalog")
BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.fornecedor")
SILVER = job_param("silver_table", f"{CATALOG}.silver.fornecedor")
KEY = "id"
SNAPSHOT = "ingestion_date"
TYPE2_COLS = ["cnpj", "razaosocial"]
TYPE1_ALLOWLIST = [
    "nomefantasia",
    "id_situacaocadastro",
    "id_tipoempresa",
    "permitenfsempedido",
    "id_tipocustocompra",
    "id_tipocustodevolucaotroca",
    "pedidominimoqtd",
    "pedidominimovalor",
    "valormaximoverbapedido",
    "id_contacontabilfinanceiro",
    "id_fornecedorfavorecido",
    "id_municipio",
]
FORBIDDEN_SILVER_COLS = {
    "senha",
    "cpfprodutorrural",
    "telefone",
    "documento",
}


bronze = spark.table(BRONZE)
silver = spark.table(SILVER)
TYPE1_COLS = [c for c in TYPE1_ALLOWLIST if c in bronze.columns]

print("\n=== GATE B7 — SUPPLIER SCD2 QUALITY GATE ===")
print(f"Bronze: {BRONZE}")
print(f"Silver: {SILVER}")
print(f"Type 2: {TYPE2_COLS}")
print(f"Type 1: {len(TYPE1_COLS)} atributo(s)\n")

checks = []


def check(name: str, failures: int, detail: str = ""):
    ok = failures == 0
    prefix = "✅" if ok else "❌"
    msg = f"{prefix} {name}"
    if detail:
        msg += f" — {detail}"
    checks.append((ok, msg))
    print(msg)


hash_expr = F.md5(
    F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TYPE2_COLS],
    )
)

w_bronze = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
expected = (
    bronze.withColumn("_expected_hash", hash_expr)
          .withColumn("_prev_hash", F.lag("_expected_hash").over(w_bronze))
          .filter(F.col("_prev_hash").isNull() | (F.col("_expected_hash") != F.col("_prev_hash")))
)
expected_rows = expected.count()
actual_rows = silver.count()
check(
    "Cardinalidade = change points Type 2 da Bronze",
    abs(actual_rows - expected_rows),
    f"expected={expected_rows:,} | actual={actual_rows:,}",
)

expected_ids = bronze.select(KEY).distinct().count()
actual_ids = silver.select(KEY).distinct().count()
check(
    "Todos os fornecedores preservados",
    abs(actual_ids - expected_ids),
    f"Bronze ids={expected_ids:,} | Silver ids={actual_ids:,}",
)

dup_grain = silver.groupBy(KEY, "valid_from").count().filter(F.col("count") > 1).count()
check("Grain única (id, valid_from)", dup_grain, f"duplicatas={dup_grain:,}")

current_per_id = (
    silver.groupBy(KEY)
          .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_count"))
)
bad_current = current_per_id.filter(F.col("current_count") != 1).count()
check("Exatamente 1 versão current por fornecedor", bad_current, f"ids inválidos={bad_current:,}")

bad_current_valid_to = silver.filter(F.col("is_current") & F.col("valid_to").isNotNull()).count()
check("Versão current tem valid_to NULL", bad_current_valid_to, f"linhas inválidas={bad_current_valid_to:,}")

bad_closed_valid_to = silver.filter((~F.col("is_current")) & F.col("valid_to").isNull()).count()
check("Versão histórica tem valid_to preenchido", bad_closed_valid_to, f"linhas inválidas={bad_closed_valid_to:,}")

w_silver = Window.partitionBy(KEY).orderBy(F.col("valid_from").asc())
intervals = silver.withColumn("_next_valid_from", F.lead("valid_from").over(w_silver))

bad_order = silver.filter(
    F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))
).count()
check("valid_from < valid_to em versões fechadas", bad_order, f"linhas inválidas={bad_order:,}")

bad_continuity = intervals.filter(~F.col("valid_to").eqNullSafe(F.col("_next_valid_from"))).count()
check("valid_to = próximo valid_from", bad_continuity, f"intervalos quebrados={bad_continuity:,}")

bad_hash = silver.withColumn("_recomputed_hash", hash_expr).filter(
    F.col("hash_versao") != F.col("_recomputed_hash")
).count()
check("hash_versao íntegro", bad_hash, f"linhas inválidas={bad_hash:,}")

same_hash_consecutive = (
    silver.withColumn("_prev_hash", F.lag("hash_versao").over(w_silver))
          .filter(F.col("_prev_hash").isNotNull() & (F.col("hash_versao") == F.col("_prev_hash")))
          .count()
)
check(
    "Nenhuma versão redundante com mesmo hash consecutivo",
    same_hash_consecutive,
    f"linhas redundantes={same_hash_consecutive:,}",
)

w_latest = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).desc())
latest = (
    bronze.withColumn("_rn", F.row_number().over(w_latest))
          .filter(F.col("_rn") == 1)
)

# Type 1 deve refletir sempre o estado atual.
latest_t1 = latest.select(
    KEY, *[F.col(c).alias(f"_latest_{c}") for c in TYPE1_COLS]
)
joined_t1 = silver.join(latest_t1, on=KEY, how="left")
type1_failures = 0
for c in TYPE1_COLS:
    mismatches = joined_t1.filter(~F.col(c).eqNullSafe(F.col(f"_latest_{c}"))).count()
    type1_failures += mismatches
check(
    "Todos os atributos Type 1 refletem o último snapshot",
    type1_failures,
    f"mismatches totais={type1_failures:,} em {len(TYPE1_COLS)} coluna(s)",
)

forbidden_present = sorted(FORBIDDEN_SILVER_COLS.intersection(set(silver.columns)))
check(
    "Data minimization: campos sensíveis/contato não estão na Silver",
    len(forbidden_present),
    f"presentes={forbidden_present}",
)

# Current deve refletir o último estado Type 2.
latest_t2 = latest.select(
    KEY, *[F.col(c).alias(f"_latest_t2_{c}") for c in TYPE2_COLS]
)
current = silver.filter(F.col("is_current")).join(latest_t2, on=KEY, how="left")
for c in TYPE2_COLS:
    mismatches = current.filter(~F.col(c).eqNullSafe(F.col(f"_latest_t2_{c}"))).count()
    check(f"Current reflete último snapshot Type 2: {c}", mismatches, f"mismatches={mismatches:,}")

invalid_source = silver.filter(
    ~F.col("valid_from_source").isin("datacadastro", "ingestion_date")
).count()
check("valid_from_source usa origem conhecida", invalid_source, f"linhas inválidas={invalid_source:,}")

# Primeira versão por fornecedor deve ser datacadastro quando a fonte é válida e não futura.
def parse_erp_timestamp(col_name: str):
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col(col_name).cast("timestamp"),
    )

first_silver = (
    silver.withColumn("_rn", F.row_number().over(w_silver))
          .filter(F.col("_rn") == 1)
          .select(KEY, "valid_from", "valid_from_source")
)
first_bronze = (
    bronze.withColumn("_rn", F.row_number().over(w_bronze))
          .filter(F.col("_rn") == 1)
          .withColumn("_created_at", parse_erp_timestamp("datacadastro"))
          .withColumn("_snapshot_ts", F.col(SNAPSHOT).cast("timestamp"))
          .select(KEY, "_created_at", "_snapshot_ts")
)
first_join = first_silver.join(first_bronze, on=KEY, how="left")
bad_first_boundary = first_join.filter(
    F.col("_created_at").isNotNull()
    & (F.to_date(F.col("_created_at")) <= F.to_date(F.col("_snapshot_ts")))
    & (
        (F.col("valid_from") != F.col("_created_at"))
        | (F.col("valid_from_source") != F.lit("datacadastro"))
    )
).count()
check("Primeira versão usa datacadastro quando confiável", bad_first_boundary, f"ids inválidos={bad_first_boundary:,}")

source_counts = {
    r["valid_from_source"]: r["count"]
    for r in silver.groupBy("valid_from_source").count().collect()
}
versioned_ids = silver.groupBy(KEY).count().filter(F.col("count") > 1).count()
max_versions = silver.groupBy(KEY).count().agg(F.max("count")).collect()[0][0]

print("\n--- RESUMO SUPPLIER SCD2 ---")
print(f"versões totais:          {actual_rows:,}")
print(f"fornecedores distintos:  {actual_ids:,}")
print(f"ids com >1 versão:       {versioned_ids:,}")
print(f"máximo versões / id:     {max_versions}")
print(f"origem de valid_from:    {source_counts}")

failed = [msg for ok, msg in checks if not ok]
print(f"\n=== RESULTADO: {len(checks) - len(failed)}/{len(checks)} checks passaram ===")
if failed:
    raise Exception("Supplier SCD2 Quality Gate falhou:\n" + "\n".join(failed))

print("✅ Fornecedor SCD2 aprovado na tabela validada.")
