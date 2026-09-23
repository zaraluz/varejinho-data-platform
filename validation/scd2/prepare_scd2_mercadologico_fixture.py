# Databricks notebook source
# validation/scd2/prepare_scd2_mercadologico_fixture.py
from pyspark.sql import functions as F

def job_param(nome, default):
    try: return dbutils.widgets.get(nome)
    except Exception: return default

CATALOG=job_param("catalog","varejinho_dev")
BRONZE=f"{CATALOG}.control._b8_mercadologico_fixture_bronze"
SILVER=f"{CATALOG}.silver._b8_mercadologico_fixture"
if not CATALOG.endswith("_dev"): raise Exception("B8 fixture só pode rodar em *_dev")
for t in [SILVER,BRONZE]: spark.sql(f"DROP TABLE IF EXISTS {t}")

rows=[
("9001","1","1","0","0","0","2","ARROZ", "2026-09-01"),
("9001","1","1","0","0","0","2","ARROZ E CEREAIS", "2026-09-02"), # Type1
("9001","1","9","0","0","0","2","ARROZ E CEREAIS", "2026-09-03"), # Type2 path
("9001","1","9","7","0","0","3","ARROZ E CEREAIS", "2026-09-04"), # Type2 level/path
("9002","5","6","3","0","0","3","REFRIGERANTE LATA", "2026-09-03"),
("9002","5","6","3","0","0","3","REFRIGERANTES LATA", "2026-09-04"), # Type1
]
schema="id string, mercadologico1 string, mercadologico2 string, mercadologico3 string, mercadologico4 string, mercadologico5 string, nivel string, descricao string, ingestion_date string"
df=spark.createDataFrame(rows,schema).withColumn("ingestion_date",F.to_date("ingestion_date"))
df.write.format("delta").mode("overwrite").saveAsTable(BRONZE)
print("\n=== GATE B8B — PREPARE MERCADOLOGICO FIXTURE ===")
print(f"Bronze: {BRONZE}\nSilver: {SILVER}\nrows={df.count()} | ids={df.select('id').distinct().count()}")
print("✅ Fixture pronta: Type1 de descrição, duas mudanças Type2 estruturais e novo id.")
