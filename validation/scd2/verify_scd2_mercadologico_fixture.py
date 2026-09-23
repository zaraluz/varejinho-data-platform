# Databricks notebook source
# validation/scd2/verify_scd2_mercadologico_fixture.py
from pyspark.sql import functions as F

def job_param(nome, default):
    try: return dbutils.widgets.get(nome)
    except Exception: return default

CATALOG=job_param("catalog","varejinho_dev")
BRONZE=f"{CATALOG}.control._b8_mercadologico_fixture_bronze"
SILVER=f"{CATALOG}.silver._b8_mercadologico_fixture"
df=spark.table(SILVER); checks=[]
def check(n,ok,d=""):
    print(("✅" if ok else "❌")+f" {n}"+(f" — {d}" if d else "")); checks.append(ok)

r9001=df.filter(F.col("id")=="9001").orderBy("valid_from").collect()
r9002=df.filter(F.col("id")=="9002").orderBy("valid_from").collect()
check("9001 tem 3 versões",len(r9001)==3,f"actual={len(r9001)}")
check("9002 tem 1 versão",len(r9002)==1,f"actual={len(r9002)}")
if len(r9001)==3:
    check("Boundaries first-observed corretos",
          [str(r["valid_from"]) for r in r9001]==["2026-09-01 00:00:00","2026-09-03 00:00:00","2026-09-04 00:00:00"])
    check("Mudança de pai cria versão",r9001[1]["mercadologico2"]=="9")
    check("Mudança de nível/caminho cria versão",r9001[2]["nivel"]=="3" and r9001[2]["mercadologico3"]=="7")
    check("Descrição Type1 propagada",all(r["descricao"]=="ARROZ E CEREAIS" for r in r9001))
if len(r9002)==1:
    check("Renomear descrição não versiona",r9002[0]["descricao"]=="REFRIGERANTES LATA")
check("Todas origens temporais são ingestion_date",df.filter(F.col("valid_from_source")!="ingestion_date").count()==0)
failed=sum(1 for x in checks if not x)
print(f"\n=== RESULTADO B8B: {len(checks)-failed}/{len(checks)} checks passaram ===")
if failed: raise Exception("Fixture B8B falhou")
for t in [SILVER,BRONZE]: spark.sql(f"DROP TABLE IF EXISTS {t}")
print("✅ Fixture mercadologico aprovada e sandbox removida.")
