# Databricks notebook source
# pipeline/silver/prepare_scd2_supplier_fixture.py
# Gate B7C — cria uma Bronze sintética isolada para provar a semântica Type 1/Type 2 de fornecedor.

from pyspark.sql import functions as F

def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default

CATALOG = job_param("catalog", "varejinho_dev")
FIXTURE_BRONZE = f"{CATALOG}.control._b7_supplier_fixture_bronze"
FIXTURE_SILVER = f"{CATALOG}.silver._b7_supplier_fixture"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B7C só pode executar em *_dev. Recebido: {CATALOG}")

spark.sql(f"DROP TABLE IF EXISTS {FIXTURE_BRONZE}")
spark.sql(f"DROP TABLE IF EXISTS {FIXTURE_SILVER}")

rows = [
    # id 900001 — primeira versão.
    ("900001","FORNECEDOR A LTDA","LOJA A","11111111000111","1","85999990000","N","3","2026/01/01 00:00:00.000000000","2026-09-01"),
    # D2 — somente Type 1: NÃO pode criar nova versão.
    ("900001","FORNECEDOR A LTDA","LOJA A NOVA","11111111000111","0","85888880000","Y","8","2026/01/01 00:00:00.000000000","2026-09-02"),
    # D3 — razão social muda: Type 2, nova versão em ingestion_date.
    ("900001","FORNECEDOR A COMERCIO LTDA","LOJA A NOVA","11111111000111","0","85888880000","Y","8","2026/01/01 00:00:00.000000000","2026-09-03"),
    # D4 — CNPJ muda: Type 2 + alerta de identidade.
    ("900001","FORNECEDOR A COMERCIO LTDA","LOJA A ATUAL","22222222000122","0","85777770000","Y","8","2026/01/01 00:00:00.000000000","2026-09-04"),

    # id 900002 — aparece depois; primeira versão vem de datacadastro.
    ("900002","FORNECEDOR B LTDA","LOJA B","33333333000133","1","85444440000","N","3","2026/09/02 08:00:00.000000000","2026-09-02"),
    # somente Type 1.
    ("900002","FORNECEDOR B LTDA","LOJA B PRIME","33333333000133","1","85333330000","Y","3","2026/09/02 08:00:00.000000000","2026-09-03"),
    ("900002","FORNECEDOR B LTDA","LOJA B PRIME","33333333000133","1","85333330000","Y","3","2026/09/02 08:00:00.000000000","2026-09-04"),
]

schema = """
id string,
razaosocial string,
nomefantasia string,
cnpj string,
id_situacaocadastro string,
telefone string,
permitenfsempedido string,
id_tipoempresa string,
datacadastro string,
ingestion_date string
"""

df = spark.createDataFrame(rows, schema=schema).withColumn("ingestion_date", F.to_date("ingestion_date"))

df.write.format("delta").mode("overwrite").saveAsTable(FIXTURE_BRONZE)

print("\n=== GATE B7C — PREPARE SUPPLIER FIXTURE ===")
print(f"Bronze sintética: {FIXTURE_BRONZE}")
print(f"Silver sandbox:   {FIXTURE_SILVER}")
print(f"linhas:           {df.count()}")
print(f"ids:              {df.select('id').distinct().count()}")
print("Cenários:")
print("  - Type 1 puro: nomefantasia/status/telefone/permitenfsempedido/id_tipoempresa")
print("  - Type 2 por razaosocial")
print("  - Type 2 por CNPJ")
print("  - novo fornecedor com datacadastro posterior")
print("✅ Fixture preparada; nenhum dado real foi alterado.")
