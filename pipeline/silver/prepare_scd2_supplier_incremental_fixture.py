# Databricks notebook source
# pipeline/silver/prepare_scd2_supplier_incremental_fixture.py
# Gate B7E — fixture que força snapshots pendentes para provar o engine incremental genérico.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
FULL_BRONZE = f"{CATALOG}.control._b7e_supplier_full_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._b7e_supplier_baseline_bronze"
SILVER = f"{CATALOG}.silver._b7e_supplier_incremental"
CONTROL = f"{CATALOG}.control._b7e_supplier_watermark"
ENTITY = "fornecedor"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B7E fixture só pode executar em *_dev. Recebido: {CATALOG}")

for table in [CONTROL, SILVER, BASELINE_BRONZE, FULL_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

rows = [
    ("900001","FORNECEDOR A LTDA","LOJA A","11111111000111","1","85999990000","N","3","1","1","10","100","500","700","900001","2304400","2026/01/01 00:00:00.000000000","2026-09-01"),
    # D2: somente Type 1.
    ("900001","FORNECEDOR A LTDA","LOJA A NOVA","11111111000111","0","85888880000","Y","8","1","1","10","100","500","700","900001","2304400","2026/01/01 00:00:00.000000000","2026-09-02"),
    # D3: Type 2 por razão social + Type 1 no fornecedor B.
    ("900001","FORNECEDOR A COMERCIO LTDA","LOJA A NOVA","11111111000111","0","85888880000","Y","8","1","1","10","100","500","700","900001","2304400","2026/01/01 00:00:00.000000000","2026-09-03"),
    # D4: Type 2 por CNPJ + Type 1.
    ("900001","FORNECEDOR A COMERCIO LTDA","LOJA A ATUAL","22222222000122","0","85777770000","Y","8","1","1","10","100","500","700","900001","2304400","2026/01/01 00:00:00.000000000","2026-09-04"),

    # Novo id em D2.
    ("900002","FORNECEDOR B LTDA","LOJA B","33333333000133","1","85444440000","N","3","2","2","20","200","600","701","900002","2304400","2026/09/02 08:00:00.000000000","2026-09-02"),
    # D3: somente Type 1.
    ("900002","FORNECEDOR B LTDA","LOJA B PRIME","33333333000133","1","85333330000","Y","3","2","2","20","200","600","701","900002","2304400","2026/09/02 08:00:00.000000000","2026-09-03"),
    ("900002","FORNECEDOR B LTDA","LOJA B PRIME","33333333000133","1","85333330000","Y","3","2","2","20","200","600","701","900002","2304400","2026/09/02 08:00:00.000000000","2026-09-04"),

    # Novo id em D4 para provar insert incremental de primeira versão.
    ("900003","FORNECEDOR C LTDA","LOJA C","44444444000144","1","85222220000","N","3","3","3","30","300","700","702","900003","2304400","2026/09/04 10:30:00.000000000","2026-09-04"),
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
id_tipocustocompra string,
id_tipocustodevolucaotroca string,
pedidominimoqtd string,
pedidominimovalor string,
valormaximoverbapedido string,
id_contacontabilfinanceiro string,
id_fornecedorfavorecido string,
id_municipio string,
datacadastro string,
ingestion_date string
"""

full = spark.createDataFrame(rows, schema=schema).withColumn("ingestion_date", F.to_date("ingestion_date"))
baseline = full.filter(F.col("ingestion_date") <= F.lit("2026-09-02").cast("date"))

full.write.format("delta").mode("overwrite").saveAsTable(FULL_BRONZE)
baseline.write.format("delta").mode("overwrite").saveAsTable(BASELINE_BRONZE)

spark.sql(f"""
    CREATE TABLE {CONTROL} (
        entity STRING,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING,
        updated_at TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CONTROL}
    VALUES ('{ENTITY}', DATE '2026-09-02', NULL, 'COMMITTED', current_timestamp())
""")

print("\n=== GATE B7E — PREPARE INCREMENTAL SUPPLIER FIXTURE ===")
print(f"Bronze baseline: {BASELINE_BRONZE}")
print(f"Bronze completa: {FULL_BRONZE}")
print(f"Silver sandbox:  {SILVER}")
print(f"Control sandbox: {CONTROL}")
print(f"baseline rows:    {baseline.count()}")
print(f"full rows:        {full.count()}")
print("watermark inicial: 2026-09-02")
print("snapshots pendentes esperados: 2026-09-03 e 2026-09-04")
print("✅ Fixture incremental preparada; nenhum dado real foi alterado.")
