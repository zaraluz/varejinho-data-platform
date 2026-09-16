# pipeline/silver/quality_gate.py
# Quality Gate da Silver — valida volumetria, schema, SCD2 e freshness
# Falha com Exception se houver erros críticos

from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone

resultados = []

def check(nome, passou, detalhe=""):
    status = "✅" if passou else "❌"
    resultados.append(f"{status} {nome} {detalhe}")

hoje = datetime.now(timezone.utc).date()
ontem = hoje - timedelta(days=1)

# ── Fatos — volumetria e freshness ──────────────────────────
FATOS = {
    "venda":                   {"bronze_min": 0.95},
    "notaentrada":             {"bronze_min": 0.40},  # extração parcial conhecida
    "notaentradaitem":         {"bronze_min": 0.95},
    "perda":                   {"bronze_min": 0.95},
    "logestoque":              {"bronze_min": 0.95},
    "promocao":                {"bronze_min": 0.08},  # full load com muita duplicata
    "promocaoitem":            {"bronze_min": 0.08},
    "pedido":                  {"bronze_min": 0.95},
    "pedidoitem":              {"bronze_min": 0.95},
    "oferta":                  {"bronze_min": 0.08},
    "pagarfornecedor":         {"bronze_min": 0.95},
    "pagarfornecedorparcela":  {"bronze_min": 0.08},  # full load histórico
    "pagaroutrasdespesas":     {"bronze_min": 0.95},
    "pagaroutrasdespesasimposto": {"bronze_min": 0.95},
}

for tabela, cfg in FATOS.items():
    try:
        bronze_count = spark.table(f"varejinho.bronze.{tabela}").count()
        silver_count = spark.table(f"varejinho.silver.{tabela}").count()

        # Volumetria
        ratio = silver_count / bronze_count if bronze_count > 0 else 0
        check(f"{tabela} — volumetria",
              ratio >= cfg["bronze_min"],
              f"(Bronze: {bronze_count:,} | Silver: {silver_count:,} | ratio: {ratio:.2%})")

        # Freshness — ingestion_date mais recente
        ultima = (spark.table(f"varejinho.silver.{tabela}")
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
        # Cada id deve ter exatamente 1 is_current = true
        multi = (spark.table(f"varejinho.silver.{dim}")
                 .filter("is_current = true")
                 .groupBy("id").count()
                 .filter("count > 1").count())
        check(f"{dim} SCD2 — 1 versão ativa por id",
              multi == 0, f"({multi} ids com múltiplas versões ativas)")

        # is_current não pode ser nulo
        nulos = (spark.table(f"varejinho.silver.{dim}")
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
                  for f in spark.table(f"varejinho.silver.{tabela}").schema.fields}
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
        quar_table = f"varejinho.silver._quarantine_{tabela}"
        if spark.catalog.tableExists(quar_table):
            count = spark.table(quar_table).count()
            check(f"{tabela} — quarentena",
                  count == 0,
                  f"({count:,} registros rejeitados nesta execução)")
    except Exception as e:
        resultados.append(f"❌ {tabela} quarentena: {str(e)[:100]}")

# ── Resultado ────────────────────────────────────────────────
print("\n=== SILVER QUALITY GATE ===\n")
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
