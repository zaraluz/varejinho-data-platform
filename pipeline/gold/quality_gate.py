# Databricks notebook source
from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


def required_param(nome: str) -> str:
    """Parâmetro obrigatório do job: falha cedo em vez de cair num default de ambiente."""
    try:
        value = dbutils.widgets.get(nome)
    except Exception:
        value = ""
    if not value:
        raise ValueError(
            f"Parâmetro obrigatório ausente: '{nome}'. Execute via job do bundle, "
            "que injeta catalog/bundle_files_path/control_root/bronze_source_catalog por target."
        )
    return value


CATALOG = required_param("catalog")
BRONZE_SOURCE_CATALOG = required_param("bronze_source_catalog")
resultados = []


def check(nome, passou, detalhe=""):
    status = "✅" if passou else "❌"
    resultados.append(f"{status} {nome} {detalhe}")


def check_temporal_mapping(
    nome,
    gold_df,
    gold_fact_key,
    gold_sk_col,
    source_df,
    source_fact_key,
    source_natural_key,
    source_event_col,
    dim_name,
    dim_natural_key,
    dim_sk_col,
    allow_null_natural_key=False,
):
    """Valida se a FK da Gold coincide exatamente com o join temporal esperado."""
    src = (
        source_df.select(
            F.col(source_fact_key).alias("_fact_key"),
            F.col(source_natural_key).alias("_natural_key"),
            F.col(source_event_col).cast("timestamp").alias("_event_ts"),
        )
    )

    invalid_event = src.filter(F.col("_event_ts").isNull()).count()
    check(
        f"{nome} — data temporal válida",
        invalid_event == 0,
        f"({invalid_event} data(s) nula(s)/inválida(s))",
    )

    dim = spark.table(f"{CATALOG}.gold.{dim_name}").select(
        F.col(dim_natural_key).alias("_dim_key"),
        F.col(dim_sk_col).alias("_expected_sk"),
        "valid_from",
        "valid_to",
    )

    joined = (
        src.alias("s")
        .join(
            dim.alias("d"),
            (F.col("s._natural_key") == F.col("d._dim_key"))
            & (F.col("s._event_ts") >= F.col("d.valid_from"))
            & (
                F.col("d.valid_to").isNull()
                | (F.col("s._event_ts") < F.col("d.valid_to"))
            ),
            "left",
        )
        .select(
            F.col("s._fact_key").alias("_fact_key"),
            F.col("s._natural_key").alias("_natural_key"),
            F.col("s._event_ts").alias("_event_ts"),
            F.col("d._expected_sk").alias("_expected_sk"),
        )
    )

    multi = (
        joined.groupBy("_fact_key")
        .agg(F.count("_expected_sk").alias("_matches"))
        .filter(F.col("_matches") > 1)
        .count()
    )
    check(
        f"{nome} — sem overlap temporal",
        multi == 0,
        f"({multi} fato(s) com múltiplas versões)",
    )

    expected = (
        joined.groupBy("_fact_key")
        .agg(F.first("_expected_sk", ignorenulls=True).alias("_expected_sk"))
    )

    actual = gold_df.select(
        F.col(gold_fact_key).alias("_fact_key"),
        F.col(gold_sk_col).alias("_actual_sk"),
    )

    comparison = actual.join(expected, on="_fact_key", how="left")
    mismatch = comparison.filter(
        ~F.col("_actual_sk").eqNullSafe(F.col("_expected_sk"))
    ).count()
    expected_nulls = comparison.filter(F.col("_expected_sk").isNull()).count()

    check(
        f"{nome} — FK temporal exata",
        mismatch == 0,
        f"({mismatch} divergência(s); {expected_nulls} SK(s) esperadas como NULL)",
    )

    first_boundary = (
        dim.groupBy("_dim_key")
        .agg(F.min("valid_from").alias("_first_valid_from"))
    )

    coverage = src.join(
        first_boundary,
        src["_natural_key"] == first_boundary["_dim_key"],
        "left",
    )

    invalid_null_key = (
        0
        if allow_null_natural_key
        else coverage.filter(F.col("_natural_key").isNull()).count()
    )
    missing_dimension = coverage.filter(
        F.col("_natural_key").isNotNull()
        & F.col("_first_valid_from").isNull()
    ).count()

    source_expected = src.join(expected, on="_fact_key", how="left").alias("se")
    boundary = first_boundary.alias("b")
    expected_with_source = (
        source_expected.join(
            boundary,
            F.col("se._natural_key") == F.col("b._dim_key"),
            "left",
        )
        .select(
            F.col("se._fact_key").alias("_fact_key"),
            F.col("se._natural_key").alias("_natural_key"),
            F.col("se._event_ts").alias("_event_ts"),
            F.col("se._expected_sk").alias("_expected_sk"),
            F.col("b._first_valid_from").alias("_first_valid_from"),
        )
    )

    unexpected_unresolved = expected_with_source.filter(
        F.col("_expected_sk").isNull()
        & F.col("_natural_key").isNotNull()
        & (
            F.col("_first_valid_from").isNull()
            | F.col("_event_ts").isNull()
            | (F.col("_event_ts") >= F.col("_first_valid_from"))
        )
    ).count()

    check(
        f"{nome} — unresolved temporal explicado",
        invalid_null_key == 0
        and missing_dimension == 0
        and unexpected_unresolved == 0,
        (
            f"(chave nula não permitida={invalid_null_key}; "
            f"sem dimensão={missing_dimension}; "
            f"gap/outro={unexpected_unresolved})"
        ),
    )


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

pedidoitem = spark.table(f"{CATALOG}.silver.pedidoitem").alias("pi")
pedido = spark.table(f"{CATALOG}.silver.pedido").alias("pe")
compras_src = (
    pedidoitem.join(pedido, F.col("pi.id_pedido") == F.col("pe.id"), "inner")
    .select(
        F.col("pi.id").alias("id_pedidoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pe.id_fornecedor").alias("id_fornecedor"),
        F.col("pe.datacompra").alias("datacompra"),
    )
)
check_temporal_mapping(
    "fato_compras → dim_produto",
    df, "id_pedidoitem", "sk_produto",
    compras_src, "id_pedidoitem", "id_produto", "datacompra",
    "dim_produto", "id_produto", "sk_produto",
)
check_temporal_mapping(
    "fato_compras → dim_fornecedor",
    df, "id_pedidoitem", "sk_fornecedor",
    compras_src, "id_pedidoitem", "id_fornecedor", "datacompra",
    "dim_fornecedor", "id_fornecedor", "sk_fornecedor",
)

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
dupes = df.groupBy("sk_promocao_item").count().filter("count > 1").count()
check("fato_promocoes — SK única", dupes == 0, f"({dupes} duplicatas)")

promocaoitem = spark.table(f"{CATALOG}.silver.promocaoitem").alias("pi")
promocao = spark.table(f"{CATALOG}.silver.promocao").alias("pr")
promocoes_src = (
    promocaoitem.join(promocao, F.col("pi.id_promocao") == F.col("pr.id"), "inner")
    .select(
        F.col("pi.id").alias("id_promocaoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pr.datainicio").alias("datainicio"),
    )
)
check_temporal_mapping(
    "fato_promocoes → dim_produto",
    df, "id_promocaoitem", "sk_produto",
    promocoes_src, "id_promocaoitem", "id_produto", "datainicio",
    "dim_produto", "id_produto", "sk_produto",
)

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

parcela = spark.table(f"{CATALOG}.silver.pagarfornecedorparcela").alias("pp")
pagarfornecedor = spark.table(f"{CATALOG}.silver.pagarfornecedor").alias("pf")
contas_src = (
    parcela.join(
        pagarfornecedor,
        F.col("pp.id_pagarfornecedor") == F.col("pf.id"),
        "inner",
    )
    .select(
        F.col("pp.id").alias("id_parcela"),
        F.col("pf.id_loja").alias("id_loja"),
        F.col("pf.id_fornecedor").alias("id_fornecedor"),
        F.col("pf.dataemissao").alias("dataemissao"),
    )
)
check_temporal_mapping(
    "fato_contas_pagar → dim_fornecedor",
    df, "id_parcela", "sk_fornecedor",
    contas_src, "id_parcela", "id_fornecedor", "dataemissao",
    "dim_fornecedor", "id_fornecedor", "sk_fornecedor",
)

silver_total = parcela.count()
eligible_keys = contas_src.select("id_parcela", "id_loja").distinct()
gold_keys = df.select("id_parcela", F.col("sk_loja").alias("id_loja")).distinct()

eligible = eligible_keys.count()
gold = gold_keys.count()
orphans = silver_total - eligible
missing_gold = eligible_keys.join(
    gold_keys,
    on=["id_parcela", "id_loja"],
    how="left_anti",
).count()
extra_gold = gold_keys.join(
    eligible_keys,
    on=["id_parcela", "id_loja"],
    how="left_anti",
).count()

check(
    "fato_contas_pagar — reconciliação elegível exata",
    gold == eligible and missing_gold == 0 and extra_gold == 0,
    (
        f"(Silver total: {silver_total:,} | elegíveis: {eligible:,} | "
        f"órfãs sem cabeçalho: {orphans:,} | Gold: {gold:,} | "
        f"missing elegível: {missing_gold:,} | extra Gold: {extra_gold:,})"
    ),
)

if orphans:
    orphan_rows = (
        parcela.join(
            pagarfornecedor.select(F.col("id").alias("_silver_parent_id")),
            F.col("pp.id_pagarfornecedor") == F.col("_silver_parent_id"),
            "left_anti",
        )
        .select(
            F.col("pp.id").alias("id_parcela"),
            F.col("pp.id_pagarfornecedor").alias("id_pagarfornecedor"),
        )
    )
    bronze_parent_ids = (
        spark.table(f"{BRONZE_SOURCE_CATALOG}.bronze.pagarfornecedor")
        .select(F.col("id").alias("_bronze_parent_id"))
        .filter(F.col("_bronze_parent_id").isNotNull())
        .distinct()
    )
    orphan_parent_present_bronze = (
        orphan_rows.join(
            bronze_parent_ids,
            F.col("id_pagarfornecedor") == F.col("_bronze_parent_id"),
            "inner",
        )
        .select(F.col("id_pagarfornecedor"))
        .distinct()
        .count()
    )
else:
    orphan_parent_present_bronze = 0

check(
    "fato_contas_pagar — órfãs explicadas pela fonte",
    orphan_parent_present_bronze == 0,
    (
        f"(órfãs Silver: {orphans:,} | parent IDs órfãos que existem na Bronze: "
        f"{orphan_parent_present_bronze:,})"
    ),
)

if orphans:
    print(
        f"⚠️ fato_contas_pagar source limitation: {orphans:,} parcela(s) Silver "
        "não possuem pagarfornecedor correspondente; nenhum parent órfão existe "
        "na Bronze atual, portanto essas linhas não são materializáveis com o grain "
        "Gold vigente."
    )

# ── fato_outras_despesas ─────────────────────────────────────
df = spark.table(f"{CATALOG}.gold.fato_outras_despesas")
dupes = df.groupBy("sk_despesa").count().filter("count > 1").count()
check("fato_outras_despesas — SK única", dupes == 0, f"({dupes} duplicatas)")

outras_src = (
    spark.table(f"{CATALOG}.silver.pagaroutrasdespesas")
    .select(
        F.col("id").alias("id_despesa"),
        "id_fornecedor",
        "dataemissao",
    )
)
check_temporal_mapping(
    "fato_outras_despesas → dim_fornecedor",
    df, "id_despesa", "sk_fornecedor",
    outras_src, "id_despesa", "id_fornecedor", "dataemissao",
    "dim_fornecedor", "id_fornecedor", "sk_fornecedor",
    allow_null_natural_key=True,
)

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
    ("dim_tipo_pagamento", "sk_tipo_pagamento"),
    ("dim_tipo_entrada", "sk_tipo_entrada"),
    ("dim_motivo_perda", "sk_motivo_perda"),
    ("dim_tipo_oferta",  "sk_tipo_oferta"),
    ("dim_promocao",     "sk_promocao"),
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

# ── star schema: domínios, chaves e hierarquia ──────────────
# O BI lê só a Gold. Cada código de domínio virou uma chave para uma dimensão ou
# uma descrição no fato; estas checagens garantem que nenhum código ficou sem
# descrição e que toda chave de fato encontra a sua dimensão.


def check_dominio(nome, fonte, coluna, dominio):
    """Todo código não nulo da fonte Silver existe no domínio (senão a descrição sairia nula)."""
    ids = spark.table(f"{CATALOG}.silver.{dominio}").select(F.col("id").cast("int").alias("_id"))
    orfaos = (
        fonte.filter(F.col(coluna).isNotNull())
        .join(ids, F.col(coluna).cast("int") == F.col("_id"), "left_anti")
        .count()
    )
    check(f"{nome} — todo código tem descrição", orfaos == 0,
          f"({orfaos} linha(s) com código sem descrição em {dominio})")


def check_fk(fato, coluna, dim, chave):
    """Toda chave não nula do fato existe na dimensão da Gold."""
    dim_keys = spark.table(f"{CATALOG}.gold.{dim}").select(F.col(chave).alias("_k"))
    orfaos = (
        spark.table(f"{CATALOG}.gold.{fato}")
        .filter(F.col(coluna).isNotNull())
        .join(dim_keys, F.col(coluna) == F.col("_k"), "left_anti")
        .count()
    )
    check(f"{fato}.{coluna} → {dim}", orfaos == 0, f"({orfaos} chave(s) sem dimensão)")


silver_t = lambda t: spark.table(f"{CATALOG}.silver.{t}")

# Descrições gravadas no próprio fato ou na dimensão
check_dominio("fato_compras.situacao_pedido", silver_t("pedido"), "id_situacaopedido", "situacaopedido")
check_dominio("fato_contas_pagar.situacao_parcela", silver_t("pagarfornecedorparcela"),
              "id_situacaopagarfornecedorparcela", "situacaopagarfornecedorparcela")
check_dominio("fato_movimento_estoque.tipo_movimentacao", silver_t("logestoque"),
              "id_tipomovimentacao", "tipomovimentacao")
check_dominio("fato_outras_despesas.situacao_despesa", silver_t("pagaroutrasdespesas"),
              "id_situacaopagaroutrasdespesas", "situacaopagaroutrasdespesas")
check_dominio("dim_produto.tipo_embalagem", silver_t("produto"), "id_tipoembalagem", "tipoembalagem")
check_dominio("dim_produto.tipo_mercadoria", silver_t("produto"), "id_tipomercadoria", "tipomercadoria")
check_dominio("dim_promocao.tipo_promocao", silver_t("promocao"), "id_tipopromocao", "tipopromocao")
check_dominio("dim_promocao.situacao_promocao", silver_t("promocao"), "id_situacaocadastro", "situacaocadastro")

# Curva ABC: 8 colunas do mesmo domínio de 3 letras, numa checagem só
curva = silver_t("curvaabc")
letras = silver_t("tipocurvaabc").select(F.col("id").cast("int").alias("_id"))
colunas_curva = [c for c in curva.columns if c.startswith("id_tipocurvaabc") and "mercadologico4" not in c
                 and "mercadologico5" not in c]
sem_letra = 0
for c in colunas_curva:
    sem_letra += (curva.filter(F.col(c).isNotNull())
                  .join(letras, F.col(c).cast("int") == F.col("_id"), "left_anti").count())
check("fato_curva_abc — toda classe tem letra", sem_letra == 0,
      f"({sem_letra} valor(es) sem letra em {len(colunas_curva)} colunas)")

# Chaves dos fatos para as dimensões novas
check_fk("fato_contas_pagar",    "sk_tipo_pagamento", "dim_tipo_pagamento", "sk_tipo_pagamento")
check_fk("fato_outras_despesas", "sk_tipo_pagamento", "dim_tipo_pagamento", "sk_tipo_pagamento")
check_fk("fato_outras_despesas", "sk_tipo_entrada",   "dim_tipo_entrada",   "sk_tipo_entrada")
check_fk("fato_perdas",          "sk_motivo_perda",   "dim_motivo_perda",   "sk_motivo_perda")
check_fk("fato_oferta",          "sk_tipo_oferta",    "dim_tipo_oferta",    "sk_tipo_oferta")
check_fk("fato_promocoes",       "sk_promocao",       "dim_promocao",       "sk_promocao")

# Hierarquia achatada: a suposição que simplificou o join (todo caminho existe na
# árvore atual) vira checagem. Se um caminho sumir, o gate falha em vez de gravar nome nulo.
sem_nome = (
    spark.table(f"{CATALOG}.gold.dim_produto")
    .filter(
        (F.col("secao").isNotNull() & F.col("secao_nome").isNull())
        | (F.col("grupo").isNotNull() & F.col("grupo_nome").isNull())
        | (F.col("subgrupo").isNotNull() & F.col("subgrupo_nome").isNull())
    )
    .count()
)
check("dim_produto — todo caminho mercadológico tem nome", sem_nome == 0,
      f"({sem_nome} versão(ões) de produto sem nome de seção/grupo/subgrupo)")

print(f"\n=== GOLD QUALITY GATE [{CATALOG}] ===\n")
for r in resultados:
    print(r)

total  = len(resultados)
passou = sum(1 for r in resultados if r.startswith("✅"))
falhou = total - passou
print(f"\n{passou}/{total} checks passaram | {falhou} falharam")

if falhou > 0:
    raise Exception(f"Gold Quality Gate falhou: {falhou}/{total} checks com erro")
