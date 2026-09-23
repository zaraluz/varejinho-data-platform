# Databricks notebook source
# validation/scd2/verify_scd2_mercadologico_incremental_fixture.py

from pyspark.sql import functions as F

def job_param(nome, default):
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default

CATALOG = job_param("catalog", "varejinho_dev")
FULL_BRONZE = f"{CATALOG}.control._b8d_mercadologico_full_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._b8d_mercadologico_baseline_bronze"
SILVER = f"{CATALOG}.silver._b8d_mercadologico_incremental"
CONTROL = f"{CATALOG}.control._b8d_mercadologico_watermark"

for t in [FULL_BRONZE, BASELINE_BRONZE, SILVER, CONTROL]:
    if not spark.catalog.tableExists(t):
        raise Exception(f"Pré-requisito ausente: {t}")

df = spark.table(SILVER)
checks = []

def check(name, ok, detail=""):
    print(("✅" if ok else "❌") + f" {name}" + (f" — {detail}" if detail else ""))
    checks.append(ok)

print("\n=== GATE B8D — VERIFY MERCADOLOGICO INCREMENTAL FIXTURE ===\n")

check("Cardinalidade final", df.count() == 4, f"expected=4 | actual={df.count()}")
check("Dois ids preservados", df.select("id").distinct().count() == 2)

r1 = df.filter(F.col("id")=="9001").orderBy("valid_from").collect()
r2 = df.filter(F.col("id")=="9002").orderBy("valid_from").collect()

check("9001 tem 3 versões", len(r1)==3, f"actual={len(r1)}")
check("9002 entrou incrementalmente com 1 versão", len(r2)==1, f"actual={len(r2)}")

if len(r1)==3:
    check(
        "Boundaries first-observed corretos",
        [str(r["valid_from"]) for r in r1] == [
            "2026-09-01 00:00:00",
            "2026-09-03 00:00:00",
            "2026-09-04 00:00:00",
        ],
        f"actual={[str(r['valid_from']) for r in r1]}",
    )
    check("Mudança de pai versionada", r1[1]["mercadologico2"]=="9")
    check("Mudança de nível/caminho versionada", r1[2]["nivel"]=="3" and r1[2]["mercadologico3"]=="7")
    check("Descrição Type 1 propagada", all(r["descricao"]=="ARROZ E CEREAIS" for r in r1))

if len(r2)==1:
    check("Descrição Type 1 do novo id atualizada sem versionar", r2[0]["descricao"]=="REFRIGERANTES LATA")
    check("Novo id usa first observed", str(r2[0]["valid_from"])=="2026-09-03 00:00:00")

wm = spark.table(CONTROL).filter(F.col("entity")=="mercadologico").collect()
wm_ok = len(wm)==1 and str(wm[0]["last_processed_snapshot"])=="2026-09-04" and wm[0]["candidate_snapshot"] is None and wm[0]["status"]=="COMMITTED"
detail = f"rows={len(wm)}" if len(wm)!=1 else f"committed={wm[0]['last_processed_snapshot']} | candidate={wm[0]['candidate_snapshot']} | status={wm[0]['status']}"
check("Watermark avançou somente após validação", wm_ok, detail)

failed = sum(1 for x in checks if not x)
print(f"\n=== RESULTADO B8D: {len(checks)-failed}/{len(checks)} checks passaram ===")
if failed:
    print("❌ Sandbox preservada para investigação.")
    raise Exception("Gate B8D falhou")

for t in [CONTROL, SILVER, BASELINE_BRONZE, FULL_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

print("✅ Incremental mercadologico aprovado e sandbox removida.")
