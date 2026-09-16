# Databricks notebook source
# pipeline/bronze/quality_gate.py
# Quality Gate da Bronze — valida volumetria, completude e duplicatas brutas
# Falha com Exception se houver alertas críticos — bloqueia pipeline no DAB

from pyspark.sql import functions as F
from datetime import datetime, timedelta

TABELAS_FATO = {
    "venda":            {"chave": "id", "data": "data"},
    "notaentrada":      {"chave": "id", "data": "dataentrada"},
    "notaentradaitem":  {"chave": "id", "data": None},
    "perda":            {"chave": "id", "data": "data"},
    "logestoque":       {"chave": "id", "data": "datamovimento"},
    "pedido":           {"chave": "id", "data": "datacompra"},
    "pedidoitem":       {"chave": "id", "data": None},
    "oferta":           {"chave": "id", "data": "datainicio"},
    "promocao":         {"chave": "id", "data": "datainicio"},
    "promocaoitem":     {"chave": "id", "data": None},
}

alertas = []
hoje = (datetime.now() - timedelta(days=1)).date()

for tabela, cfg in TABELAS_FATO.items():
    df = spark.table(f"varejinho.bronze.{tabela}")

    # 1. Volumetria — partição de ontem existe?
    ultima = df.agg(F.max("ingestion_date")).collect()[0][0]
    if ultima < hoje:
        alertas.append(f"⚠️ [{tabela}] Última partição: {ultima} — pode estar desatualizada")

    # 2. Completude — nulos na chave
    nulos_chave = df.where(F.col(cfg["chave"]).isNull()).count()
    if nulos_chave > 0:
        alertas.append(f"⚠️ [{tabela}] {nulos_chave} nulos em '{cfg['chave']}'")

    # 3. ingestion_date sempre preenchido
    sem_particao = df.where(F.col("ingestion_date").isNull()).count()
    if sem_particao > 0:
        alertas.append(f"⚠️ [{tabela}] {sem_particao} registros sem ingestion_date")

    # 4. Duplicata bruta na última partição
    df_ultima = df.where(F.col("ingestion_date") == ultima)
    total     = df_ultima.count()
    distintos = df_ultima.select(cfg["chave"]).distinct().count()
    if total != distintos:
        alertas.append(f"⚠️ [{tabela}] {total - distintos} duplicatas brutas na partição {ultima}")

    print(f"✅ {tabela} — última partição: {ultima}, {total:,} linhas")

print(f"\n=== {len(alertas)} alertas ===")
for a in alertas:
    print(a)

# Falha o pipeline se houver alertas críticos
if alertas:
    raise Exception(f"Bronze Quality Gate falhou: {len(alertas)} alertas detectados")