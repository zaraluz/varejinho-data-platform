# Databricks notebook source
# pipeline/silver/diagnose_fact_file_versions.py
# Gate D5B — prova file-level da mutabilidade da Bronze raw em S3/UC.
# Read-only. Lê diretamente a external table do catálogo fonte para expor _metadata.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
ENTITY = job_param("entity", "notaentradaitem")
CONTROL_TABLE = job_param(
    "control_table",
    f"{CATALOG}.control.fact_watermark",
)

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D5B só pode executar em *_dev. Recebido: {CATALOG}")

SOURCE = f"{BRONZE_SOURCE_CATALOG}.bronze.{ENTITY}"

state = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity") == ENTITY)
    .collect()
)
if len(state) != 1:
    raise Exception(f"{ENTITY}: esperado 1 watermark; encontrado={len(state)}")

row = state[0]
committed = row["last_processed_snapshot"]
candidate = row["candidate_snapshot"]
status = row["status"]

print("\n=== GATE D5B — BRONZE FILE VERSION DIAGNOSTIC ===")
print(f"source external table: {SOURCE}")
print(f"committed={committed} | candidate={candidate} | status={status}")

# _metadata precisa ser projetado diretamente da external table; a view Bronze
# de dev usa SELECT * e, portanto, não carrega a coluna oculta.
raw = spark.sql(f"""
    SELECT
        *,
        _metadata.file_path AS _file_path,
        _metadata.file_name AS _file_name,
        _metadata.file_size AS _file_size,
        _metadata.file_modification_time AS _file_modified_at
    FROM {SOURCE}
""")

bronze_max = raw.agg(F.max("ingestion_date")).collect()[0][0]
print(f"Bronze max atual: {bronze_max}")

print("\n--- FILES POR INGESTION_DATE ---")
files = (
    raw.groupBy(
        "ingestion_date",
        "_file_path",
        "_file_name",
        "_file_size",
        "_file_modified_at",
    )
    .agg(F.count("*").alias("rows"))
    .withColumn(
        "_modified_date",
        F.to_date("_file_modified_at"),
    )
    .withColumn(
        "_days_after_ingestion",
        F.datediff(F.col("_modified_date"), F.col("ingestion_date")),
    )
    .orderBy("ingestion_date", "_file_path")
)

files.show(500, truncate=False)

summary = (
    files.groupBy("ingestion_date")
    .agg(
        F.countDistinct("_file_path").alias("files"),
        F.sum("rows").alias("rows"),
        F.min("_file_modified_at").alias("first_file_modified_at"),
        F.max("_file_modified_at").alias("last_file_modified_at"),
        F.max("_days_after_ingestion").alias("max_days_after_ingestion"),
    )
    .orderBy("ingestion_date")
)

print("\n--- RESUMO POR INGESTION_DATE ---")
summary.show(500, truncate=False)

late_files = files.filter(F.col("_days_after_ingestion") > 0)
late_count = late_files.count()

print(
    "\nArquivos cujo modification date é posterior ao ingestion_date lógico: "
    f"{late_count:,}"
)
if late_count:
    late_files.show(200, truncate=False)

# ID que apareceu no expected atual, mas não estava na Silver produzida a partir
# do baseline/APPLY anterior.
missing_id = "3657528"
print(f"\n--- METADATA DO ID MISSING {missing_id} ---")
(
    raw.filter(F.col("id") == F.lit(missing_id))
    .select(
        "id",
        "ingestion_date",
        "_file_path",
        "_file_name",
        "_file_size",
        "_file_modified_at",
    )
    .orderBy("ingestion_date", "_file_path")
    .show(100, truncate=False)
)

# Amostra de IDs que estavam na Silver e sumiram totalmente da Bronze atual.
extra_ids = [
    "3652711", "3652715", "3652717", "3652722", "3652727",
    "3652728", "3652730", "3652733", "3652737", "3653101",
]

print("\n--- BUSCA DOS EXTRAS NA BRONZE EXTERNAL ATUAL ---")
extra_found = (
    raw.filter(F.col("id").isin(extra_ids))
    .select(
        "id",
        "ingestion_date",
        "_file_path",
        "_file_name",
        "_file_size",
        "_file_modified_at",
    )
    .orderBy("id", "ingestion_date")
)

extra_found_count = extra_found.count()
print(
    f"IDs da amostra encontrados hoje na Bronze raw: "
    f"{extra_found_count}/{len(extra_ids)}"
)
extra_found.show(100, truncate=False)

print("\n--- INTERPRETAÇÃO ---")
if late_count > 0:
    print(
        "⚠️ Há arquivos cuja modificação física ocorreu depois do ingestion_date "
        "que eles carregam. ingestion_date não é um identificador imutável de lote."
    )
else:
    print(
        "ℹ️ Os arquivos atuais não exibem modification_date posterior ao "
        "ingestion_date. Isso não desfaz a evidência de remoção de linhas; "
        "apenas não prova reescrita tardia via timestamp atual."
    )

missing_meta = (
    raw.filter(F.col("id") == F.lit(missing_id))
    .select("ingestion_date", "_file_modified_at")
    .collect()
)
if missing_meta:
    for m in missing_meta:
        print(
            f"ID {missing_id}: ingestion_date={m['ingestion_date']} | "
            f"file_modified_at={m['_file_modified_at']}"
        )

if extra_found_count == 0:
    print(
        "⚠️ A amostra de chaves extras da Silver está completamente ausente "
        "da external table atual, consistente com remoção/substituição do raw."
    )

print("\n✅ Gate D5B é read-only. Nenhum dado ou watermark foi alterado.")
