# Databricks notebook source
# pipeline/silver/build_scd2_product.py
# Gate B3 — reconstrução determinística do histórico SCD2 de produto a partir dos snapshots Bronze.
# IMPORTANTE: durante o hardening este notebook só aceita catálogo *_dev.

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

# Decisão de modeling — 17/09/2026.
# Type 2: mudança cria nova versão histórica.
TYPE2_COLS = [
    "descricaocompleta",
    "mercadologico1",
    "mercadologico2",
    "mercadologico3",
    "ncm1",
    "id_tipoembalagem",
]

# Type 1: mudança NÃO cria versão; o último valor conhecido sobrescreve
# esse atributo em todas as versões da entidade.
TYPE1_COLS = [
    "descricaoreduzida",
    "id_tipomercadoria",  # provisório até semântica do ERP ser confirmada
    "pesoliquido",
    "pesobruto",
]


def parse_erp_timestamp(col_name: str):
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col(col_name).cast("timestamp"),
    )


print("\n=== GATE B3 — BUILD PRODUCT SCD2 ===")
print(f"Fonte:   {BRONZE}")
print(f"Destino: {SILVER}")
print(f"Type 2:  {TYPE2_COLS}")
print(f"Type 1:  {TYPE1_COLS}\n")

# Proteção explícita: este backfill é experimental e não pode tocar produção.
if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Proteção de hardening: build_scd2_product só pode executar em catálogo *_dev. Recebido: {CATALOG}"
    )

raw = spark.table(BRONZE)
source_cols = set(raw.columns)
required = {KEY, SNAPSHOT, "datacadastro", "dataalteracao", *TYPE2_COLS, *TYPE1_COLS}
missing = sorted(required - source_cols)
if missing:
    raise Exception(f"Colunas obrigatórias ausentes na Bronze produto: {missing}")

# Guardrails da fonte. O profiling já mostrou zero ocorrências, mas a implementação
# não depende de memória humana: falha se a premissa deixar de ser verdadeira.
null_keys = raw.filter(F.col(KEY).isNull()).count()
if null_keys:
    raise Exception(f"Natural key nula em {null_keys:,} linha(s) de {BRONZE}")

duplicate_groups = (
    raw.groupBy(KEY, SNAPSHOT)
       .count()
       .filter(F.col("count") > 1)
       .count()
)
if duplicate_groups:
    raise Exception(
        f"Grain inválido: {duplicate_groups:,} grupo(s) duplicado(s) por ({KEY}, {SNAPSHOT})"
    )

# Fingerprint SOMENTE dos atributos Type 2 definidos no modeling.
hash_expr = F.md5(
    F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
            for c in TYPE2_COLS
        ],
    )
)

base = (
    raw.withColumn("hash_versao", hash_expr)
       .withColumn("_snapshot_ts", F.col(SNAPSHOT).cast("timestamp"))
       .withColumn("_created_at", parse_erp_timestamp("datacadastro"))
       .withColumn("_altered_at", parse_erp_timestamp("dataalteracao"))
)

# 1) LAG: detecta change points por produto.
w_hist = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
with_prev = base.withColumn("_prev_hash", F.lag("hash_versao").over(w_hist))
change_points = with_prev.filter(
    F.col("_prev_hash").isNull() | (F.col("hash_versao") != F.col("_prev_hash"))
)

# 2) Política temporal.
# Primeira versão conhecida: datacadastro quando válida; fallback = primeiro snapshot.
# Versões seguintes: dataalteracao quando válida; fallback = primeiro snapshot em que vimos a mudança.
change_points = change_points.withColumn(
    "_valid_from_candidate",
    F.when(
        F.col("_prev_hash").isNull(),
        F.when(
            F.col("_created_at").isNotNull() & (F.col("_created_at") <= F.col("_snapshot_ts")),
            F.col("_created_at"),
        ).otherwise(F.col("_snapshot_ts")),
    ).otherwise(
        F.when(
            F.col("_altered_at").isNotNull() & (F.col("_altered_at") <= F.col("_snapshot_ts")),
            F.col("_altered_at"),
        ).otherwise(F.col("_snapshot_ts")),
    ),
)

# Se o timestamp do ERP quebrar a ordem temporal, fazemos fallback conservador
# para a data em que a versão foi observada. Isso evita intervalos invertidos.
w_changes = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
change_points = change_points.withColumn(
    "_prev_valid_from_candidate",
    F.lag("_valid_from_candidate").over(w_changes),
)
change_points = change_points.withColumn(
    "valid_from",
    F.when(
        F.col("_prev_valid_from_candidate").isNotNull()
        & (F.col("_valid_from_candidate") <= F.col("_prev_valid_from_candidate")),
        F.col("_snapshot_ts"),
    ).otherwise(F.col("_valid_from_candidate")),
)

# 3) Type 1: pega o último estado conhecido e aplica a TODAS as versões do produto.
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

# 4) LEAD: a próxima versão define o fim exclusivo da versão atual.
w_versions = Window.partitionBy(KEY).orderBy(F.col("valid_from").asc(), F.col(SNAPSHOT).asc())
versions = (
    versions.withColumn("valid_to", F.lead("valid_from").over(w_versions))
            .withColumn("is_current", F.col("valid_to").isNull())
)

# Metadados explícitos de observabilidade da reconstrução.
versions = (
    versions.withColumn("scd_source_snapshot", F.col(SNAPSHOT).cast("date"))
            .withColumn(
                "valid_from_source",
                F.when(
                    F.col("_prev_hash").isNull()
                    & F.col("_created_at").isNotNull()
                    & (F.col("valid_from") == F.col("_created_at")),
                    F.lit("datacadastro"),
                ).when(
                    F.col("_prev_hash").isNotNull()
                    & F.col("_altered_at").isNotNull()
                    & (F.col("valid_from") == F.col("_altered_at")),
                    F.lit("dataalteracao"),
                ).otherwise(F.lit("ingestion_date")),
            )
)

helper_cols = [
    "_snapshot_ts",
    "_created_at",
    "_altered_at",
    "_prev_hash",
    "_valid_from_candidate",
    "_prev_valid_from_candidate",
]
versions = versions.drop(*helper_cols)

# Este é um BACKFILL determinístico a partir dos snapshots preservados.
# A manutenção incremental futura será implementada depois que o modelo for validado.
(
    versions.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(SILVER)
)

rows = spark.table(SILVER).count()
ids = spark.table(SILVER).select(KEY).distinct().count()
current = spark.table(SILVER).filter(F.col("is_current")).count()
versioned_ids = (
    spark.table(SILVER).groupBy(KEY).count().filter(F.col("count") > 1).count()
)

print("\n=== BUILD CONCLUÍDO ===")
print(f"linhas/versionamentos: {rows:,}")
print(f"ids distintos:         {ids:,}")
print(f"versões atuais:        {current:,}")
print(f"ids com >1 versão:     {versioned_ids:,}")
print("Próxima etapa obrigatória: executar validate_scd2_product antes de integrar ao pipeline diário.")
