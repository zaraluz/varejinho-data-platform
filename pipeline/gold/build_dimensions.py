# Databricks notebook source
# pipeline/gold/build_dimensions.py
# Orquestrador das dimensões da Gold


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
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")

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
