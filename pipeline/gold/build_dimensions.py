# Databricks notebook source
# pipeline/gold/build_dimensions.py
# Orquestrador das dimensões da Gold


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)

DIMENSOES = [
    "dim_tempo",
    "dim_produto",
    "dim_fornecedor",
    "dim_loja",
    "dim_mercadologico",
]

for dim in DIMENSOES:
    path = f"{BUNDLE_FILES_PATH}/pipeline/gold/{dim}.sql"
    sql = open(path, encoding="utf-8").read()

    # Ponte de compatibilidade: os SQLs serão convertidos para placeholder
    # explícito no gate de revisão temporal da Gold. Até lá, a substituição
    # exata abaixo garante isolamento real entre dev/prod sem depender da Git Folder.
    sql = sql.replace("varejinho.", f"{CATALOG}.")

    spark.sql(sql)
    count = spark.table(f"{CATALOG}.gold.{dim}").count()
    print(f"✅ {CATALOG}.gold.{dim}: {count:,} linhas")
