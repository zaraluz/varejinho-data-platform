# Databricks notebook source
from pyspark.sql import functions as F


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


# ── fato_vendas ──────────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_vendas")
dupes = df.groupBy("sk_venda").count().filter("count > 1").count()
check("fato_vendas — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_produto IS NULL").count()
check("fato_vendas — join dim_produto", nulls == 0, f"({nulls} sem produto)")

neg = df.filter("valor_total < 0").count()
check("fato_vendas — valor_total >= 0", neg == 0, f"({neg} negativos)")

silver = spark.table(f"{CATALOG}.silver.venda").count()
gold   = df.count()
check("fato_vendas — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

anos = df.select("ano").distinct().orderBy("ano").collect()
check("fato_vendas — partições", len(anos) > 0,
      f"({[r.ano for r in anos]})")

# ── fato_compras ─────────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_compras")
dupes = df.groupBy("sk_compra").count().filter("count > 1").count()
check("fato_compras — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls_prod = df.filter("sk_produto IS NULL").count()
check("fato_compras — join dim_produto", nulls_prod == 0, f"({nulls_prod} sem produto)")

nulls_forn = df.filter("sk_fornecedor IS NULL").count()
check("fato_compras — join dim_fornecedor", nulls_forn == 0, f"({nulls_forn} sem fornecedor)")

silver = spark.table(f"{CATALOG}.silver.pedidoitem").count()
gold   = df.count()
check("fato_compras — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_perdas ──────────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_perdas")
dupes = df.groupBy("sk_perda").count().filter("count > 1").count()
check("fato_perdas — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_produto IS NULL").count()
check("fato_perdas — join dim_produto", nulls == 0, f"({nulls} sem produto)")

silver = spark.table(f"{CATALOG}.silver.perda").count()
gold   = df.count()
check("fato_perdas — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_movimento_estoque ───────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_movimento_estoque")
dupes = df.groupBy("sk_movimento").count().filter("count > 1").count()
check("fato_movimento_estoque — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_produto IS NULL").count()
pct   = round(nulls / df.count() * 100, 2)
check("fato_movimento_estoque — join dim_produto < 1%", pct < 1,
      f"({nulls} sem produto = {pct}%)")

silver = spark.table(f"{CATALOG}.silver.logestoque").count()
gold   = df.count()
check("fato_movimento_estoque — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_promocoes ───────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_promocoes")
dupes = df.groupBy("sk_promocao").count().filter("count > 1").count()
check("fato_promocoes — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_produto IS NULL").count()
check("fato_promocoes — join dim_produto", nulls == 0, f"({nulls} sem produto)")

silver = spark.table(f"{CATALOG}.silver.promocaoitem").count()
gold   = df.count()
check("fato_promocoes — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_oferta ──────────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_oferta")
dupes = df.groupBy("sk_oferta").count().filter("count > 1").count()
check("fato_oferta — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_produto IS NULL").count()
check("fato_oferta — join dim_produto", nulls == 0, f"({nulls} sem produto)")

silver = spark.table(f"{CATALOG}.silver.oferta").count()
gold   = df.count()
check("fato_oferta — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_contas_pagar ────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_contas_pagar")
dupes = df.groupBy("sk_parcela").count().filter("count > 1").count()
check("fato_contas_pagar — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_fornecedor IS NULL").count()
check("fato_contas_pagar — join dim_fornecedor", nulls == 0, f"({nulls} sem fornecedor)")

silver = spark.table(f"{CATALOG}.silver.pagarfornecedorparcela").count()
gold   = df.count()
check("fato_contas_pagar — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_outras_despesas ─────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_outras_despesas")
dupes = df.groupBy("sk_despesa").count().filter("count > 1").count()
check("fato_outras_despesas — SK única", dupes == 0, f"({dupes} duplicatas)")

silver = spark.table(f"{CATALOG}.silver.pagaroutrasdespesas").count()
gold   = df.count()
check("fato_outras_despesas — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── fato_curva_abc ───────────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_curva_abc")
dupes = df.groupBy("sk_curva").count().filter("count > 1").count()
check("fato_curva_abc — SK única", dupes == 0, f"({dupes} duplicatas)")

nulls = df.filter("sk_produto IS NULL").count()
check("fato_curva_abc — join dim_produto", nulls == 0, f"({nulls} sem produto)")

silver = spark.table(f"{CATALOG}.silver.curvaabc").count()
gold   = df.count()
check("fato_curva_abc — volumetria", gold >= silver * 0.99,
      f"(Silver: {silver:,} | Gold: {gold:,})")

# ── dimensões ────────────────────────────────────────────────
for dim, chave in [
    ("dim_produto",      "sk_produto"),
    ("dim_fornecedor",   "sk_fornecedor"),
    ("dim_mercadologico","sk_mercadologico"),
    ("dim_loja",         "sk_loja"),
    ("dim_tempo",        "sk_tempo"),
]:
    df = spark.table(f"{CATALOG}.gold.{dim}")
    dupes = df.groupBy(chave).count().filter("count > 1").count()
    check(f"{dim} — SK única", dupes == 0, f"({dupes} duplicatas)")

multi_current = (spark.table(f"{CATALOG}.gold.dim_produto")
    .filter("is_current = true")
    .groupBy("id_produto").count()
    .filter("count > 1").count())
check("dim_produto — no máximo 1 versão atual por produto",
      multi_current == 0, f"({multi_current} com múltiplas versões ativas)")

print(f"\n=== GOLD QUALITY GATE [{CATALOG}] ===\n")
for r in resultados:
    print(r)

total  = len(resultados)
passou = sum(1 for r in resultados if r.startswith("✅"))
falhou = total - passou
print(f"\n{passou}/{total} checks passaram | {falhou} falharam")

if falhou > 0:
    raise Exception(f"Gold Quality Gate falhou: {falhou}/{total} checks com erro")
