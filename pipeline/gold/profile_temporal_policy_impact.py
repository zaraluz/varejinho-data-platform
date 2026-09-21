# Databricks notebook source
# pipeline/gold/profile_temporal_policy_impact.py
# Gate G3 — impacto read-only da política temporal estrita na Gold.
#
# Objetivo:
# - NÃO alterar Silver/Gold.
# - Medir quantas linhas deixam de receber SK quando o join deixa de usar is_current.
# - Quantificar o peso financeiro dos casos temporalmente não resolvidos quando há métrica aditiva.
# - Produzir evidência para escolher representação (ex.: membro UNKNOWN_TEMPORAL) sem fabricar história.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate G3 só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
    )


DIM_PRODUTO = spark.table(f"{CATALOG}.gold.dim_produto").select(
    "sk_produto", "id_produto", "valid_from", "valid_to", "is_current"
)
DIM_FORNECEDOR = spark.table(f"{CATALOG}.gold.dim_fornecedor").select(
    "sk_fornecedor", "id_fornecedor", "valid_from", "valid_to", "is_current"
)


def temporal_policy_impact(
    label,
    facts,
    fact_key,
    fact_natural_key,
    event_date,
    dim,
    dim_natural_key,
    dim_sk,
    amount_col=None,
):
    print(f"\n{'=' * 100}")
    print(f"{label} | event_date={event_date}")

    selected = [
        F.col(fact_key).alias("_fact_key"),
        F.col(fact_natural_key).alias("_id"),
        F.col(event_date).alias("_event_raw"),
    ]
    if amount_col:
        selected.append(F.col(amount_col).alias("_amount_raw"))

    base = (
        facts.select(*selected)
        .withColumn("_event_ts", F.expr("try_cast(_event_raw as timestamp)"))
    )
    if amount_col:
        base = base.withColumn(
            "_amount",
            F.expr("try_cast(_amount_raw as decimal(38,6))"),
        )

    current = (
        dim.filter(F.col("is_current") == True)
        .select(
            F.col(dim_natural_key).alias("_id"),
            F.col(dim_sk).alias("_sk_current"),
        )
    )
    current_joined = base.join(current, on="_id", how="left")

    temporal = (
        base.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f._id") == F.col(f"d.{dim_natural_key}"))
            & (F.col("f._event_ts") >= F.col("d.valid_from"))
            & (
                F.col("d.valid_to").isNull()
                | (F.col("f._event_ts") < F.col("d.valid_to"))
            ),
            how="left",
        )
        .select(
            F.col("f._fact_key").alias("_fact_key"),
            F.col(f"d.{dim_sk}").alias("_sk_temporal"),
        )
    )

    multi = (
        temporal.groupBy("_fact_key")
        .agg(F.count("_sk_temporal").alias("_matches"))
        .filter(F.col("_matches") > 1)
        .count()
    )
    if multi:
        raise Exception(f"{label}: {multi:,} fact(s) com múltiplos matches temporais")

    temporal_one = (
        temporal.groupBy("_fact_key")
        .agg(F.first("_sk_temporal", ignorenulls=True).alias("_sk_temporal"))
    )

    comparison = current_joined.join(temporal_one, on="_fact_key", how="left")

    total = comparison.count()
    strict_unmatched = comparison.filter(F.col("_sk_temporal").isNull()).count()
    current_unmatched = comparison.filter(F.col("_sk_current").isNull()).count()
    changed = comparison.filter(
        F.col("_sk_current").isNotNull()
        & F.col("_sk_temporal").isNotNull()
        & (F.col("_sk_current") != F.col("_sk_temporal"))
    ).count()

    coverage = (
        dim.groupBy(dim_natural_key)
        .agg(F.min("valid_from").alias("_first_valid_from"))
        .select(
            F.col(dim_natural_key).alias("_id"),
            "_first_valid_from",
        )
    )

    unresolved = (
        comparison.filter(
            F.col("_event_ts").isNotNull()
            & F.col("_sk_current").isNotNull()
            & F.col("_sk_temporal").isNull()
        )
        .join(coverage, on="_id", how="left")
        .withColumn(
            "_reason",
            F.when(
                F.col("_first_valid_from").isNotNull()
                & (F.col("_event_ts") < F.col("_first_valid_from")),
                F.lit("BEFORE_FIRST_VERSION"),
            ).otherwise(F.lit("OTHER_UNRESOLVED")),
        )
    )

    reason_counts = {
        r["_reason"]: r["count"]
        for r in unresolved.groupBy("_reason").count().collect()
    }

    strict_pct = (strict_unmatched / total * 100) if total else 0
    changed_pct = (changed / total * 100) if total else 0

    print(f"rows:                         {total:,}")
    print(f"current join null:            {current_unmatched:,}")
    print(f"strict temporal unresolved:   {strict_unmatched:,} ({strict_pct:.6f}%)")
    print(f"current SK != temporal SK:    {changed:,} ({changed_pct:.6f}%)")
    print(f"unresolved reasons:           {reason_counts}")

    total_amount = None
    unresolved_amount = None
    unresolved_amount_pct = None

    if amount_col:
        amount_stats = comparison.agg(
            F.sum("_amount").alias("total_amount"),
            F.sum(
                F.when(F.col("_sk_temporal").isNull(), F.col("_amount"))
                .otherwise(F.lit(0))
            ).alias("unresolved_amount"),
        ).collect()[0]

        total_amount = amount_stats["total_amount"]
        unresolved_amount = amount_stats["unresolved_amount"]
        if total_amount not in (None, 0):
            unresolved_amount_pct = float(unresolved_amount / total_amount * 100)
        else:
            unresolved_amount_pct = 0.0

        print(f"total amount:                 {total_amount}")
        print(
            "unresolved amount:            "
            f"{unresolved_amount} ({unresolved_amount_pct:.6f}%)"
        )

    if strict_unmatched:
        print("Sample unresolved facts:")
        cols = [
            "_fact_key",
            "_id",
            "_event_ts",
            "_first_valid_from",
            "_reason",
        ]
        if amount_col:
            cols.append("_amount")
        (
            unresolved.select(*cols)
            .orderBy(F.col("_event_ts").asc(), F.col("_fact_key").asc())
            .limit(20)
            .show(truncate=False)
        )

    return {
        "label": label,
        "event_date": event_date,
        "rows": total,
        "current_null": current_unmatched,
        "strict_unresolved": strict_unmatched,
        "strict_unresolved_pct": strict_pct,
        "changed": changed,
        "changed_pct": changed_pct,
        "reason_counts": reason_counts,
        "total_amount": str(total_amount) if total_amount is not None else None,
        "unresolved_amount": str(unresolved_amount) if unresolved_amount is not None else None,
        "unresolved_amount_pct": unresolved_amount_pct,
    }


results = []

# 1) Compras -> produto e fornecedor.
pedidoitem = spark.table(f"{CATALOG}.silver.pedidoitem").alias("pi")
pedido = spark.table(f"{CATALOG}.silver.pedido").alias("pe")
compras = (
    pedidoitem.join(pedido, F.col("pi.id_pedido") == F.col("pe.id"), "inner")
    .select(
        F.col("pi.id").alias("id_pedidoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pe.id_fornecedor").alias("id_fornecedor"),
        F.col("pe.datacompra").alias("datacompra"),
        F.col("pi.valortotal").alias("valortotal"),
    )
)

results.append(
    temporal_policy_impact(
        "fato_compras -> dim_produto",
        compras,
        "id_pedidoitem",
        "id_produto",
        "datacompra",
        DIM_PRODUTO,
        "id_produto",
        "sk_produto",
        amount_col="valortotal",
    )
)
results.append(
    temporal_policy_impact(
        "fato_compras -> dim_fornecedor",
        compras,
        "id_pedidoitem",
        "id_fornecedor",
        "datacompra",
        DIM_FORNECEDOR,
        "id_fornecedor",
        "sk_fornecedor",
        amount_col="valortotal",
    )
)

# 2) Promoções -> produto. Não somamos preço/valor porque não é medida aditiva no grão.
promocaoitem = spark.table(f"{CATALOG}.silver.promocaoitem").alias("pi")
promocao = spark.table(f"{CATALOG}.silver.promocao").alias("pr")
promos = (
    promocaoitem.join(promocao, F.col("pi.id_promocao") == F.col("pr.id"), "inner")
    .select(
        F.col("pi.id").alias("id_promocaoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pr.datainicio").alias("datainicio"),
    )
)

results.append(
    temporal_policy_impact(
        "fato_promocoes -> dim_produto",
        promos,
        "id_promocaoitem",
        "id_produto",
        "datainicio",
        DIM_PRODUTO,
        "id_produto",
        "sk_produto",
    )
)

# 3) Contas a pagar -> fornecedor.
parcela = spark.table(f"{CATALOG}.silver.pagarfornecedorparcela").alias("pp")
cab = spark.table(f"{CATALOG}.silver.pagarfornecedor").alias("pf")
contas = (
    parcela.join(cab, F.col("pp.id_pagarfornecedor") == F.col("pf.id"), "inner")
    .select(
        F.col("pp.id").alias("id_parcela"),
        F.col("pf.id_fornecedor").alias("id_fornecedor"),
        F.col("pf.dataemissao").alias("dataemissao"),
        F.col("pp.valor").alias("valor"),
    )
)

results.append(
    temporal_policy_impact(
        "fato_contas_pagar -> dim_fornecedor",
        contas,
        "id_parcela",
        "id_fornecedor",
        "dataemissao",
        DIM_FORNECEDOR,
        "id_fornecedor",
        "sk_fornecedor",
        amount_col="valor",
    )
)

# 4) Outras despesas -> fornecedor.
outras = (
    spark.table(f"{CATALOG}.silver.pagaroutrasdespesas")
    .select(
        F.col("id").alias("id_despesa"),
        "id_fornecedor",
        "dataemissao",
        "valor",
    )
)

results.append(
    temporal_policy_impact(
        "fato_outras_despesas -> dim_fornecedor",
        outras,
        "id_despesa",
        "id_fornecedor",
        "dataemissao",
        DIM_FORNECEDOR,
        "id_fornecedor",
        "sk_fornecedor",
        amount_col="valor",
    )
)

print("\n\n=== GATE G3 — TEMPORAL POLICY IMPACT SUMMARY ===")
print(
    "relationship | event_date | rows | current_null | strict_unresolved | "
    "strict_unresolved_pct | changed | changed_pct | reasons | "
    "total_amount | unresolved_amount | unresolved_amount_pct"
)
for r in results:
    print(
        f"{r['label']} | {r['event_date']} | {r['rows']} | {r['current_null']} | "
        f"{r['strict_unresolved']} | {r['strict_unresolved_pct']:.6f} | "
        f"{r['changed']} | {r['changed_pct']:.6f} | {r['reason_counts']} | "
        f"{r['total_amount']} | {r['unresolved_amount']} | "
        f"{r['unresolved_amount_pct']}"
    )

other_unresolved = [
    r for r in results
    if any(k != "BEFORE_FIRST_VERSION" and v > 0 for k, v in r["reason_counts"].items())
]
if other_unresolved:
    raise Exception(
        "Gate G3 encontrou unresolved temporal fora de BEFORE_FIRST_VERSION: "
        + ", ".join(r["label"] for r in other_unresolved)
    )

print("\n✅ G3 concluído em modo read-only.")
print("ℹ️ O gate mede impacto; não cria membro unknown e não altera joins da Gold.")
