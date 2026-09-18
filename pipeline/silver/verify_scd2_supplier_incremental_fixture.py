# Databricks notebook source
# pipeline/silver/verify_scd2_supplier_incremental_fixture.py
# Gate B7E — verifica o resultado do processamento incremental e limpa sandbox após sucesso.

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

for table in [FULL_BRONZE, BASELINE_BRONZE, SILVER, CONTROL]:
    if not spark.catalog.tableExists(table):
        raise Exception(f"Pré-requisito ausente: {table}")

df = spark.table(SILVER)
checks = []


def check(name, ok, detail=""):
    prefix = "✅" if ok else "❌"
    msg = f"{prefix} {name}" + (f" — {detail}" if detail else "")
    checks.append((ok, msg))
    print(msg)


print("\n=== GATE B7E — VERIFY INCREMENTAL SUPPLIER FIXTURE ===\n")

check("Cardinalidade final", df.count() == 5, f"expected=5 | actual={df.count()}")
check("Três fornecedores preservados", df.select("id").distinct().count() == 3)

counts = {r["id"]: r["count"] for r in df.groupBy("id").count().collect()}
check("900001 tem 3 versões", counts.get("900001") == 3, f"actual={counts.get('900001')}")
check("900002 continua com 1 versão", counts.get("900002") == 1, f"actual={counts.get('900002')}")
check("900003 entrou incrementalmente com 1 versão", counts.get("900003") == 1, f"actual={counts.get('900003')}")

v1 = df.filter(F.col("id") == "900001").orderBy("valid_from").collect()
if len(v1) == 3:
    check(
        "900001 boundaries incrementais corretos",
        [str(r["valid_from"]) for r in v1]
        == ["2026-01-01 00:00:00", "2026-09-03 00:00:00", "2026-09-04 00:00:00"],
        f"actual={[str(r['valid_from']) for r in v1]}",
    )
    check(
        "900001 origens temporais corretas",
        [r["valid_from_source"] for r in v1]
        == ["datacadastro", "ingestion_date", "ingestion_date"],
        f"actual={[r['valid_from_source'] for r in v1]}",
    )

# Type 1 final deve ser propagado para todas as versões.
for col_name, expected in {
    "nomefantasia": "LOJA A ATUAL",
    "id_situacaocadastro": "0",
    "telefone": "85777770000",
    "permitenfsempedido": "Y",
    "id_tipoempresa": "8",
}.items():
    mismatches = df.filter(F.col("id") == "900001").filter(
        ~F.col(col_name).eqNullSafe(F.lit(expected))
    ).count()
    check(f"Type 1 final propagado: {col_name}", mismatches == 0, f"mismatches={mismatches}")

v2 = df.filter(F.col("id") == "900002").collect()
if len(v2) == 1:
    check(
        "900002 Type 1 atualizado sem versionar",
        v2[0]["nomefantasia"] == "LOJA B PRIME"
        and v2[0]["telefone"] == "85333330000"
        and v2[0]["permitenfsempedido"] == "Y",
    )

v3 = df.filter(F.col("id") == "900003").collect()
if len(v3) == 1:
    check(
        "900003 preserva hora de datacadastro",
        str(v3[0]["valid_from"]) == "2026-09-04 10:30:00"
        and v3[0]["valid_from_source"] == "datacadastro",
        f"valid_from={v3[0]['valid_from']} | source={v3[0]['valid_from_source']}",
    )

wm_rows = spark.table(CONTROL).filter(F.col("entity") == ENTITY).collect()
wm_ok = False
wm_detail = f"rows={len(wm_rows)}"
if len(wm_rows) == 1:
    w = wm_rows[0]
    wm_ok = (
        str(w["last_processed_snapshot"]) == "2026-09-04"
        and w["candidate_snapshot"] is None
        and w["status"] == "COMMITTED"
    )
    wm_detail = (
        f"committed={w['last_processed_snapshot']} | "
        f"candidate={w['candidate_snapshot']} | status={w['status']}"
    )
check("Watermark avançou somente após validação", wm_ok, wm_detail)

failed = [msg for ok, msg in checks if not ok]
print(f"\n=== RESULTADO B7E: {len(checks)-len(failed)}/{len(checks)} checks passaram ===")

if failed:
    print("❌ Sandbox preservada para investigação.")
    raise Exception("Gate B7E fixture falhou:\n" + "\n".join(failed))

for table in [CONTROL, SILVER, BASELINE_BRONZE, FULL_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("✅ Incremental supplier fixture aprovada.")
print("✅ Tabelas sandbox removidas após sucesso.")
