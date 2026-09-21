# Databricks notebook source
# pipeline/silver/diagnose_fact_partition_maturity.py
# Gate D5C — diagnóstico read-only para descobrir a regra real de fechamento
# das partições diárias no S3.
#
# Objetivo:
# - medir, para cada fact, quantos dias separam ingestion_date e file_modified_at
# - identificar se partições históricas fecham em D, D+1 ou mais tarde
# - NÃO altera Bronze, Silver ou watermarks

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D5C só pode executar em *_dev. Recebido: {CATALOG}")

FACTS = [
    "notaentrada",
    "notaentradaitem",
    "perda",
    "logestoque",
    "promocao",
    "promocaoitem",
    "pedido",
    "pedidoitem",
    "oferta",
    "pagarfornecedor",
    "pagarfornecedorparcela",
    "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto",
]

print("\n=== GATE D5C — FACT PARTITION MATURITY ===")
print(f"source catalog: {BRONZE_SOURCE_CATALOG}")
print("Read-only: nenhuma tabela ou watermark será alterado.\n")

summary = []

for entity in FACTS:
    source = f"{BRONZE_SOURCE_CATALOG}.bronze.{entity}"

    raw = spark.sql(f"""
        SELECT
            ingestion_date,
            _metadata.file_path AS file_path,
            _metadata.file_modification_time AS file_modified_at
        FROM {source}
    """)

    files = (
        raw.groupBy("ingestion_date", "file_path", "file_modified_at")
        .agg(F.count("*").alias("rows"))
        .withColumn("modified_date", F.to_date("file_modified_at"))
        .withColumn(
            "lag_days",
            F.datediff(F.col("modified_date"), F.col("ingestion_date")),
        )
    )

    stats = files.agg(
        F.countDistinct("ingestion_date").alias("partitions"),
        F.min("ingestion_date").alias("min_ingestion"),
        F.max("ingestion_date").alias("max_ingestion"),
        F.min("lag_days").alias("min_lag"),
        F.max("lag_days").alias("max_lag"),
        F.expr("percentile_approx(lag_days, 0.5)").alias("median_lag"),
        F.sum(F.when(F.col("lag_days") <= 0, 1).otherwise(0)).alias("files_d"),
        F.sum(F.when(F.col("lag_days") == 1, 1).otherwise(0)).alias("files_d1"),
        F.sum(F.when(F.col("lag_days") > 1, 1).otherwise(0)).alias("files_gt_d1"),
    ).collect()[0]

    latest = (
        files.orderBy(F.col("ingestion_date").desc(), F.col("file_modified_at").desc())
        .limit(3)
        .collect()
    )

    print(f"\n{'=' * 92}")
    print(f"TABLE: {entity}")
    print(
        f"Partitions={stats['partitions']} | "
        f"{stats['min_ingestion']} -> {stats['max_ingestion']}"
    )
    print(
        f"Lag days: min={stats['min_lag']} | median={stats['median_lag']} | "
        f"max={stats['max_lag']}"
    )
    print(
        f"Files by maturity: D-or-earlier={stats['files_d']} | "
        f"D+1={stats['files_d1']} | >D+1={stats['files_gt_d1']}"
    )
    print("Latest partitions/files:")
    for r in latest:
        print(
            f"  ingestion_date={r['ingestion_date']} | "
            f"modified={r['file_modified_at']} | "
            f"lag_days={r['lag_days']} | rows={r['rows']:,}"
        )

    status = "REVIEW" if (stats["files_gt_d1"] or 0) > 0 else "D_PLUS_1_COMPATIBLE"

    summary.append(
        (
            entity,
            status,
            stats["partitions"],
            stats["min_lag"],
            stats["median_lag"],
            stats["max_lag"],
            stats["files_gt_d1"],
            stats["max_ingestion"],
        )
    )

print("\n\n=== GATE D5C — SUMMARY ===")
print(
    "table | status | partitions | min_lag | median_lag | max_lag | "
    "files_gt_d1 | latest_ingestion"
)
for row in summary:
    print(" | ".join("n/a" if v is None else str(v) for v in row))

review = [r[0] for r in summary if r[1] == "REVIEW"]

if review:
    print(
        "\n⚠️ Existem tabelas com arquivos modificados depois de D+1: "
        + ", ".join(review)
    )
    print(
        "Não assumir fechamento D+1 global; essas tabelas precisam de política específica."
    )
else:
    print(
        "\n✅ Nenhuma fact possui arquivo atualmente visível com modification date > D+1."
    )
    print(
        "Isso é compatível com uma política de partição madura em D+1, "
        "mas ainda exige decisão operacional explícita antes de mudar watermarks."
    )

print("\nGate D5C é diagnóstico. Nenhum estado foi alterado.")
