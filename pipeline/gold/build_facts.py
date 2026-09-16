# Databricks notebook source
# pipeline/gold/gold_fatos.py
# Orquestrador dos fatos da Gold
# Lê e executa cada SQL versionado no repo
# Task do DAB: roda após silver_fatos e gold_dimensoes

REPO = "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform"

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
    path = f"{REPO}/pipeline/gold/{fato}.sql"
    sql  = open(path).read()
    spark.sql(sql)
    count = spark.table(f"varejinho.gold.{fato}").count()
    print(f"✅ {fato}: {count:,} linhas")
