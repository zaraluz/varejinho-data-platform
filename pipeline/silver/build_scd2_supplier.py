# Databricks notebook source
# pipeline/silver/build_scd2_supplier.py
# Gate B7C/B7D — reconstrução determinística do histórico SCD2 de fornecedor.
# Pode receber tabelas sandbox por parâmetro; durante o hardening só aceita catálogo *_dev.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.fornecedor")
SILVER = job_param("silver_table", f"{CATALOG}.silver.fornecedor")
KEY = "id"
SNAPSHOT = "ingestion_date"

# Modeling aprovado em 18/09/2026:
# - identity-bearing: mudança preserva uma nova versão histórica;
# - CNPJ também merece alerta/auditoria se mudar sob o mesmo id ERP.
TYPE2_COLS = ["cnpj", "razaosocial"]

# Data minimization: somente atributos explicitamente aprovados entram na Silver.
# Campos de contato pessoal, credenciais, dados bancários/documentais e demais
# colunas da Bronze NÃO são copiados por padrão.
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


def parse_erp_timestamp(col_name: str):
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col(col_name).cast("timestamp"),
    )


if not CATALOG.endswith("_dev"):
    raise Exception(f"build_scd2_supplier só pode executar em *_dev. Recebido: {CATALOG}")

raw = spark.table(BRONZE)
source_cols = set(raw.columns)
required = {KEY, SNAPSHOT, "datacadastro", *TYPE2_COLS, *TYPE1_ALLOWLIST}
missing = sorted(required - source_cols)
if missing:
    raise Exception(f"Colunas obrigatórias ausentes em {BRONZE}: {missing}")

TYPE1_COLS = [c for c in TYPE1_ALLOWLIST if c in source_cols]

print("\n=== GATE B7 — BUILD SUPPLIER SCD2 ===")
print(f"Fonte:   {BRONZE}")
print(f"Destino: {SILVER}")
print(f"Type 2:  {TYPE2_COLS}")
print(f"Type 1:  {len(TYPE1_COLS)} atributo(s) de current-state\n")

null_keys = raw.filter(F.col(KEY).isNull()).count()
if null_keys:
    raise Exception(f"Natural key nula em {null_keys:,} linha(s)")

duplicate_groups = (
    raw.groupBy(KEY, SNAPSHOT).count().filter(F.col("count") > 1).count()
)
if duplicate_groups:
    raise Exception(
        f"Grain inválido: {duplicate_groups:,} grupo(s) duplicado(s) por ({KEY}, {SNAPSHOT})"
    )

hash_expr = F.md5(
    F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TYPE2_COLS],
    )
)

base = (
    raw.withColumn("hash_versao", hash_expr)
       .withColumn("_snapshot_ts", F.col(SNAPSHOT).cast("timestamp"))
       .withColumn("_created_at", parse_erp_timestamp("datacadastro"))
)

w_hist = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
with_prev = (
    base.withColumn("_prev_hash", F.lag("hash_versao").over(w_hist))
        .withColumn("_prev_cnpj", F.lag("cnpj").over(w_hist))
)

change_points = with_prev.filter(
    F.col("_prev_hash").isNull() | (F.col("hash_versao") != F.col("_prev_hash"))
)

# Primeira versão: datacadastro quando confiável; fallback para primeiro snapshot.
# Versões posteriores: ingestion_date, pois fornecedor não possui dataalteracao.
change_points = change_points.withColumn(
    "valid_from",
    F.when(
        F.col("_prev_hash").isNull(),
        F.when(
            F.col("_created_at").isNotNull()
            & (F.to_date(F.col("_created_at")) <= F.col(SNAPSHOT).cast("date")),
            F.col("_created_at"),
        ).otherwise(F.col("_snapshot_ts")),
    ).otherwise(F.col("_snapshot_ts")),
)

change_points = change_points.withColumn(
    "valid_from_source",
    F.when(
        F.col("_prev_hash").isNull()
        & F.col("_created_at").isNotNull()
        & (F.to_date(F.col("_created_at")) <= F.col(SNAPSHOT).cast("date"))
        & (F.col("valid_from") == F.col("_created_at")),
        F.lit("datacadastro"),
    ).otherwise(F.lit("ingestion_date")),
)

# CNPJ sob o mesmo id ERP é identity-bearing: preservar a versão e sinalizar.
cnpj_changes = change_points.filter(
    F.col("_prev_cnpj").isNotNull() & (~F.col("cnpj").eqNullSafe(F.col("_prev_cnpj")))
).count()
if cnpj_changes:
    print(f"⚠️ ALERTA DE IDENTIDADE: {cnpj_changes:,} mudança(s) de CNPJ sob o mesmo id ERP.")

# Type 1 verdadeiro: último estado conhecido é aplicado a todas as versões.
w_latest = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).desc())
latest_type1 = (
    raw.withColumn("_latest_rn", F.row_number().over(w_latest))
       .filter(F.col("_latest_rn") == 1)
       .select(
           F.col(KEY),
           *[F.col(c).alias(f"_type1_{c}") for c in TYPE1_COLS],
       )
)

cp_cols_without_type1 = [c for c in change_points.columns if c not in TYPE1_COLS]
versions = change_points.select(*cp_cols_without_type1).join(latest_type1, on=KEY, how="left")
for c in TYPE1_COLS:
    versions = versions.withColumn(c, F.col(f"_type1_{c}")).drop(f"_type1_{c}")

w_versions = Window.partitionBy(KEY).orderBy(F.col("valid_from").asc(), F.col(SNAPSHOT).asc())
versions = (
    versions.withColumn("valid_to", F.lead("valid_from").over(w_versions))
            .withColumn("is_current", F.col("valid_to").isNull())
            .withColumn("scd_source_snapshot", F.col(SNAPSHOT).cast("date"))
)

helper_cols = ["_snapshot_ts", "_created_at", "_prev_hash", "_prev_cnpj"]
versions = versions.drop(*helper_cols)

# Schema curado: não deixar a largura da Bronze vazar para a Silver.
OUTPUT_COLS = [
    KEY, *TYPE2_COLS, *TYPE1_COLS, "datacadastro",
    "hash_versao", "valid_from", "valid_to", "is_current",
    "scd_source_snapshot", "valid_from_source",
]
versions = versions.select(*OUTPUT_COLS)

(
    versions.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(SILVER)
)

out = spark.table(SILVER)
rows = out.count()
ids = out.select(KEY).distinct().count()
current = out.filter(F.col("is_current")).count()
versioned_ids = out.groupBy(KEY).count().filter(F.col("count") > 1).count()

print("\n=== BUILD SUPPLIER CONCLUÍDO ===")
print(f"linhas/versionamentos: {rows:,}")
print(f"ids distintos:         {ids:,}")
print(f"versões atuais:        {current:,}")
print(f"ids com >1 versão:     {versioned_ids:,}")
print(f"mudanças de CNPJ:      {cnpj_changes:,}")
print("Próxima etapa obrigatória: Quality Gate de fornecedor.")
