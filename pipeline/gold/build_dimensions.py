# Databricks notebook source
# pipeline/gold/gold_dimensoes.py
# Orquestrador das dimensões da Gold
# Lê e executa cada SQL versionado no repo
# Task do DAB: roda após silver_scd2

import os

REPO = "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform"

DIMENSOES = [
    "dim_tempo",
    "dim_produto",
    "dim_fornecedor",
    "dim_loja",
    "dim_mercadologico",
]

for dim in DIMENSOES:
    path = f"{REPO}/pipeline/gold/{dim}.sql"
    sql  = open(path).read()
    spark.sql(sql)
    count = spark.table(f"varejinho.gold.{dim}").count()
    print(f"✅ {dim}: {count:,} linhas")
