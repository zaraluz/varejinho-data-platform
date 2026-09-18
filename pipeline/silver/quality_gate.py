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
resultados = []


def check(nome, passou, detalhe=""):
    status = "✅" if passou else "❌"
    resultados.append(f"{status} {nome} {detalhe}")


hoje = datetime.now(timezone.utc).date()
ontem = hoje - timedelta(days=1)

# ── Fatos — volumetria e freshness ──────────────────────────
FATOS = {
    "venda":                   {"mode": "row_ratio", "bronze_min": 0.95},
    "notaentrada":             {"mode": "row_ratio", "bronze_min": 0.40},  # extração parcial conhecida
    "notaentradaitem":         {"mode": "row_ratio", "bronze_min": 0.95},
    "perda":                   {"mode": "row_ratio", "bronze_min": 0.95},
    "logestoque":              {"mode": "row_ratio", "bronze_min": 0.95},

    # Full-load diário: a Bronze acumula o mesmo id em vários snapshots,
    # enquanto a Silver mantém apenas o último estado por chave.
    # Comparar Silver / linhas brutas da Bronze faz o ratio cair a cada novo
    # snapshot e inevitavelmente gera falso positivo. Aqui a cobertura correta
    # é por chave distinta observada em toda a Bronze.
    "promocao":                {"mode": "distinct_keys", "keys": ["id"]},
    "promocaoitem":            {"mode": "distinct_keys", "keys": ["id"]},
    "oferta":                  {"mode": "distinct_keys", "keys": ["id"]},
    "pagarfornecedorparcela":  {"mode": "distinct_keys", "keys": ["id"]},

    "pedido":                  {"mode": "row_ratio", "bronze_min": 0.95},
    "pedidoitem":              {"mode": "row_ratio", "bronze_min": 0.95},
    "pagarfornecedor":         {"mode": "row_ratio", "bronze_min": 0.95},
    "pagaroutrasdespesas":     {"mode": "row_ratio", "bronze_min": 0.95},
    "pagaroutrasdespesasimposto": {"mode": "row_ratio", "bronze_min": 0.95},
}

for tabela, cfg in FATOS.items():
    try:
        bronze_df = spark.table(f"{CATALOG}.bronze.{tabela}")
        silver_df = spark.table(f"{CATALOG}.silver.{tabela}")

        if cfg["mode"] == "distinct_keys":
            keys = cfg["keys"]
            bronze_raw = bronze_df.count()
            bronze_keys = bronze_df.select(*keys).distinct().count()
            silver_keys = silver_df.select(*keys).distinct().count()
            missing_keys = (
                bronze_df.select(*keys).distinct()
                .join(silver_df.select(*keys).distinct(), on=keys, how="left_anti")
                .count()
            )
            extra_keys = (
                silver_df.select(*keys).distinct()
                .join(bronze_df.select(*keys).distinct(), on=keys, how="left_anti")
                .count()
            )
            check(
                f"{tabela} — cobertura por chave distinta",
                missing_keys == 0 and extra_keys == 0,
                f"(Bronze raw: {bronze_raw:,} | Bronze keys: {bronze_keys:,} | "
                f"Silver keys: {silver_keys:,} | missing: {missing_keys:,} | extra: {extra_keys:,})",
            )
        else:
            bronze_count = bronze_df.count()
            silver_count = silver_df.count()
            ratio = silver_count / bronze_count if bronze_count > 0 else 0
            check(
                f"{tabela} — volumetria",
                ratio >= cfg["bronze_min"],
                f"(Bronze: {bronze_count:,} | Silver: {silver_count:,} | ratio: {ratio:.2%})",
            )

        ultima = (spark.table(f"{CATALOG}.silver.{tabela}")
                  .agg(F.max("ingestion_date")).collect()[0][0])
        if ultima:
            ultima_date = ultima if isinstance(ultima, type(hoje)) else ultima.date() if hasattr(ultima, 'date') else None
            if ultima_date:
                check(f"{tabela} — freshness",
                      ultima_date >= ontem,
                      f"(última partição: {ultima_date})")

    except Exception as e:
        resultados.append(f"❌ {tabela}: {str(e)[:100]}")

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
