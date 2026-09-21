# Databricks notebook source
# pipeline/silver/quality_gate.py
# Quality Gate da Silver — valida volumetria, schema, SCD2 e freshness
# Falha com Exception se houver erros críticos

from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
FACT_WATERMARK = f"{CATALOG}.control.fact_watermark"
resultados = []


def check(nome, passou, detalhe=""):
    status = "✅" if passou else "❌"
    resultados.append(f"{status} {nome} {detalhe}")


hoje = datetime.now(timezone.utc).date()
ontem = hoje - timedelta(days=1)

# ── Fatos incrementais por partição madura ────────────────────────────────

# Venda e as 13 facts transacionais/financeiras usam o mesmo invariant diário:
# watermark único e COMMITTED, candidate limpo, committed == mature_cutoff
# e nenhuma linha da partição ainda aberta presente na Silver.

INCREMENTAL_FACTS = [
    "venda",
    "notaentrada",
    "notaentradaitem",
    "perda",
    "logestoque",
    "promocao",
    "promocaoitem",
    "pedido",
    "pedidoitem",
    "oferta",
    "pagarfornecedor",
    "pagarfornecedorparcela",
    "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto",
]

# Para essas 14 tabelas, o invariant diário não é Silver/full Bronze ratio.
# Gates D5C e D7A provaram que a partição do dia D permanece aberta até D+1.
# O QG agora valida o estado operacional incremental:
#   - watermark existe e está COMMITTED;
#   - candidate está limpo;
#   - committed == maior partição fisicamente madura;
#   - Silver não contém ingestion_date acima desse cutoff.
for tabela in INCREMENTAL_FACTS:
    try:
        source = f"{BRONZE_SOURCE_CATALOG}.bronze.{tabela}"

        maturity = spark.sql(f"""
            SELECT
                ingestion_date,
                MIN(to_date(_metadata.file_modification_time)) AS min_modified_date
            FROM {source}
            GROUP BY ingestion_date
        """)

        mature_cutoff = (
            maturity
            .filter(F.col("min_modified_date") > F.col("ingestion_date"))
            .agg(F.max("ingestion_date").alias("mature_cutoff"))
            .collect()[0]["mature_cutoff"]
        )

        states = (
            spark.table(FACT_WATERMARK)
            .filter(F.col("entity") == tabela)
            .collect()
        )

        check(
            f"{tabela} — watermark único",
            len(states) == 1,
            f"(rows de controle: {len(states)})",
        )

        if len(states) != 1:
            continue

        state = states[0]
        committed = state["last_processed_snapshot"]
        candidate = state["candidate_snapshot"]
        status = state["status"]

        state_ok = (
            mature_cutoff is not None
            and status == "COMMITTED"
            and candidate is None
            and committed == mature_cutoff
        )

        check(
            f"{tabela} — alinhado à partição madura",
            state_ok,
            f"(committed: {committed} | mature_cutoff: {mature_cutoff} | "
            f"candidate: {candidate} | status: {status})",
        )

        silver_max = (
            spark.table(f"{CATALOG}.silver.{tabela}")
            .agg(F.max("ingestion_date").alias("max_ingestion_date"))
            .collect()[0]["max_ingestion_date"]
        )

        no_future = (
            mature_cutoff is not None
            and (silver_max is None or silver_max <= mature_cutoff)
        )

        check(
            f"{tabela} — sem partição aberta na Silver",
            no_future,
            f"(Silver max ingestion_date: {silver_max} | mature_cutoff: {mature_cutoff})",
        )

    except Exception as e:
        resultados.append(f"❌ {tabela} incremental QG: {str(e)[:200]}")

# ── SCD2 — integridade das dimensões ────────────────────────
for dim in ["produto", "fornecedor", "mercadologico"]:
    try:
        multi = (spark.table(f"{CATALOG}.silver.{dim}")
                 .filter("is_current = true")
                 .groupBy("id").count()
                 .filter("count > 1").count())
        check(f"{dim} SCD2 — no máximo 1 versão ativa por id",
              multi == 0, f"({multi} ids com múltiplas versões ativas)")

        nulos = (spark.table(f"{CATALOG}.silver.{dim}")
                 .filter("is_current IS NULL").count())
        check(f"{dim} SCD2 — is_current não nulo",
              nulos == 0, f"({nulos} registros com is_current NULL)")

    except Exception as e:
        resultados.append(f"❌ {dim} SCD2: {str(e)[:100]}")

# ── Schema — colunas críticas com tipo correto ───────────────
SCHEMA_CHECKS = {
    "venda":      [("valor_total", "decimal"), ("data", "timestamp")],
    "logestoque": [("quantidade", "decimal"), ("datamovimento", "timestamp")],
    "oferta":     [("precooferta", "decimal"), ("datainicio", "timestamp")],
    "pedidoitem": [("quantidade", "decimal"), ("custocompra", "decimal")],
}

for tabela, cols in SCHEMA_CHECKS.items():
    try:
        schema = {f.name: f.dataType.simpleString()
                  for f in spark.table(f"{CATALOG}.silver.{tabela}").schema.fields}
        for col_name, tipo_esperado in cols:
            tipo_real = schema.get(col_name, "ausente")
            check(f"{tabela}.{col_name} — tipo correto",
                  tipo_esperado in tipo_real,
                  f"(esperado: {tipo_esperado} | real: {tipo_real})")
    except Exception as e:
        resultados.append(f"❌ {tabela} schema: {str(e)[:100]}")

# ── Quarentena — valida somente o snapshot da execução atual ─
QUARENTENAS = [
    "venda", "notaentrada", "notaentradaitem", "perda", "logestoque",
    "promocao", "promocaoitem", "pedido", "pedidoitem", "oferta",
    "pagarfornecedor", "pagarfornecedorparcela", "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto"
]

for tabela in QUARENTENAS:
    try:
        quar_table = f"{CATALOG}.silver._quarantine_{tabela}"
        if spark.catalog.tableExists(quar_table):
            count = spark.table(quar_table).count()
            check(f"{tabela} — quarentena",
                  count == 0,
                  f"({count:,} registros rejeitados nesta execução)")
    except Exception as e:
        resultados.append(f"❌ {tabela} quarentena: {str(e)[:100]}")

print(f"\n=== SILVER QUALITY GATE [{CATALOG}] ===\n")
for r in resultados:
    print(r)

total  = len(resultados)
passou = sum(1 for r in resultados if r.startswith("✅"))
falhas = [r for r in resultados if r.startswith("❌")]
falhou = len(falhas)
print(f"\n{passou}/{total} checks passaram | {falhou} falharam")

if falhas:
    raise Exception(
        "Silver Quality Gate falhou:\n" + "\n".join(falhas)
    )
