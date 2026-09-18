# Databricks notebook source
# pipeline/silver/verify_scd2_supplier_fixture.py
# Gate B7C — prova semântica da fixture e limpa as tabelas sandbox somente após sucesso.

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

for table in [FIXTURE_BRONZE, FIXTURE_SILVER]:
    if not spark.catalog.tableExists(table):
        raise Exception(f"Fixture ausente: {table}")

df = spark.table(FIXTURE_SILVER)
checks = []

def check(name, ok, detail=""):
    prefix = "✅" if ok else "❌"
    msg = f"{prefix} {name}" + (f" — {detail}" if detail else "")
    checks.append((ok, msg))
    print(msg)

print("\n=== GATE B7C — VERIFY SUPPLIER FIXTURE ===\n")

rows = df.count()
ids = df.select("id").distinct().count()
check("Cardinalidade esperada", rows == 4, f"expected=4 | actual={rows}")
check("Dois fornecedores preservados", ids == 2, f"actual={ids}")

v1 = df.filter(F.col("id") == "900001").orderBy("valid_from")
v2 = df.filter(F.col("id") == "900002").orderBy("valid_from")

v1_rows = v1.collect()
v2_rows = v2.collect()

check("Fornecedor 900001 tem 3 versões", len(v1_rows) == 3, f"actual={len(v1_rows)}")
check("Fornecedor 900002 tem 1 versão", len(v2_rows) == 1, f"actual={len(v2_rows)}")

if len(v1_rows) == 3:
    valid_froms = [str(r["valid_from"]) for r in v1_rows]
    sources = [r["valid_from_source"] for r in v1_rows]
    check(
        "Boundaries do 900001",
        valid_froms == ["2026-01-01 00:00:00", "2026-09-03 00:00:00", "2026-09-04 00:00:00"],
        f"actual={valid_froms}",
    )
    check(
        "Origem temporal do 900001",
        sources == ["datacadastro", "ingestion_date", "ingestion_date"],
        f"actual={sources}",
    )
    check(
        "Versão 2 nasceu da mudança de razão social",
        v1_rows[1]["razaosocial"] == "FORNECEDOR A COMERCIO LTDA"
        and v1_rows[1]["cnpj"] == "11111111000111",
    )
    check(
        "Versão 3 nasceu da mudança de CNPJ",
        v1_rows[2]["cnpj"] == "22222222000122",
        f"actual={v1_rows[2]['cnpj']}",
    )

# Type 1 deve mostrar o estado MAIS RECENTE em todas as versões.
t1_expected = {
    "nomefantasia": "LOJA A ATUAL",
    "id_situacaocadastro": "0",
    "telefone": "85777770000",
    "permitenfsempedido": "Y",
    "id_tipoempresa": "8",
}
for col_name, expected in t1_expected.items():
    mismatches = v1.filter(~F.col(col_name).eqNullSafe(F.lit(expected))).count()
    check(
        f"Type 1 propagado em todas as versões: {col_name}",
        mismatches == 0,
        f"mismatches={mismatches}",
    )

if len(v2_rows) == 1:
    check(
        "Novo fornecedor usa datacadastro",
        str(v2_rows[0]["valid_from"]) == "2026-09-02 08:00:00"
        and v2_rows[0]["valid_from_source"] == "datacadastro",
        f"valid_from={v2_rows[0]['valid_from']} | source={v2_rows[0]['valid_from_source']}",
    )
    check(
        "Type 1 do fornecedor 900002 não criou nova versão",
        v2_rows[0]["nomefantasia"] == "LOJA B PRIME"
        and v2_rows[0]["permitenfsempedido"] == "Y"
        and v2_rows[0]["telefone"] == "85333330000",
    )

failed = [msg for ok, msg in checks if not ok]
print(f"\n=== RESULTADO B7C: {len(checks)-len(failed)}/{len(checks)} checks passaram ===")

if failed:
    print("❌ Fixture preservada para investigação.")
    raise Exception("Gate B7C falhou:\n" + "\n".join(failed))

spark.sql(f"DROP TABLE IF EXISTS {FIXTURE_SILVER}")
spark.sql(f"DROP TABLE IF EXISTS {FIXTURE_BRONZE}")

print("✅ Fixture sintética aprovada.")
print("✅ Tabelas sandbox removidas após sucesso.")
