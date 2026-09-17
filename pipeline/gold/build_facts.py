# Databricks notebook source
# pipeline/gold/build_facts.py
# Orquestrador dos fatos da Gold


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform",
)

FATOS = [
    "fato_vendas",
    "fato_compras",
    "fato_perdas",
    "fato_movimento_estoque",
    "fato_promocoes",
    "fato_oferta",
    "fato_contas_pagar",
    "fato_outras_despesas",
    "fato_curva_abc",
]

for fato in FATOS:
    path = f"{BUNDLE_FILES_PATH}/pipeline/gold/{fato}.sql"
    sql = open(path, encoding="utf-8").read()

    # Ponte de compatibilidade até os SQLs receberem placeholder explícito
    # no gate de revisão temporal da Gold.
    sql = sql.replace("varejinho.", f"{CATALOG}.")

    spark.sql(sql)
    count = spark.table(f"{CATALOG}.gold.{fato}").count()
    print(f"✅ {CATALOG}.gold.{fato}: {count:,} linhas")
