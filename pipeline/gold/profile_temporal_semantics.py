# Databricks notebook source
# pipeline/gold/profile_temporal_semantics.py
# Gate G1 — auditoria read-only dos joins temporais candidatos na Gold.
#
# Objetivo:
# - NÃO alterar Gold.
# - Quantificar onde o join atual por is_current difere do join temporal.
# - Medir nulls e múltiplas versões candidatas por data de negócio.
# - Não escolher semântica automaticamente: produz evidência para a decisão.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate G1 só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
    )

DIM_PRODUTO = spark.table(f"{CATALOG}.gold.dim_produto").select(
    "sk_produto", "id_produto", "valid_from", "valid_to", "is_current"
)
DIM_FORNECEDOR = spark.table(f"{CATALOG}.gold.dim_fornecedor").select(
    "sk_fornecedor", "id_fornecedor", "valid_from", "valid_to", "is_current"
)


def current_map(dim, natural_key, sk):
    return (
        dim.filter(F.col("is_current") == True)
           .select(
               F.col(natural_key).alias("_id"),
               F.col(sk).alias("_sk_current"),
           )
    )


def temporal_profile(
    label,
    facts,
    fact_key,
    fact_natural_key,
    event_date,
    dim,
    dim_natural_key,
    dim_sk,
):
    print(f"\n{'=' * 100}")
    print(f"{label} | event_date={event_date}")

    base = (
        facts
        .select(
            F.col(fact_key).alias("_fact_key"),
            F.col(fact_natural_key).alias("_id"),
            F.col(event_date).alias("_event_ts"),
        )
    )

    current = current_map(dim, dim_natural_key, dim_sk)
    current_joined = base.join(current, on="_id", how="left")

    temporal = (
        base.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f._id") == F.col(f"d.{dim_natural_key}"))
            & (F.col("f._event_ts") >= F.col("d.valid_from"))
            & (
                F.col("f._event_ts")
                < F.coalesce(
                    F.col("d.valid_to"),
                    F.lit("2999-12-31 00:00:00").cast("timestamp"),
                )
            ),
            how="left",
        )
        .select(
            F.col("f._fact_key").alias("_fact_key"),
            F.col("f._id").alias("_id"),
            F.col("f._event_ts").alias("_event_ts"),
            F.col(f"d.{dim_sk}").alias("_sk_temporal"),
        )
    )

    temporal_multi = (
        temporal.groupBy("_fact_key")
        .agg(F.count(F.col("_sk_temporal")).alias("_matches"))
        .filter(F.col("_matches") > 1)
        .count()
    )

    temporal_one = (
        temporal.groupBy("_fact_key", "_id", "_event_ts")
        .agg(F.first("_sk_temporal", ignorenulls=True).alias("_sk_temporal"))
    )

    comparison = (
        current_joined.join(
            temporal_one.select("_fact_key", "_sk_temporal"),
            on="_fact_key",
            how="left",
        )
    )

    total = comparison.count()
    null_event = comparison.filter(F.col("_event_ts").isNull()).count()
    null_current = comparison.filter(F.col("_sk_current").isNull()).count()
    null_temporal = comparison.filter(F.col("_sk_temporal").isNull()).count()
    changed = comparison.filter(
        F.col("_sk_current").isNotNull()
        & F.col("_sk_temporal").isNotNull()
        & (F.col("_sk_current") != F.col("_sk_temporal"))
    ).count()

    pct = (changed / total * 100) if total else 0

    print(f"rows:                         {total:,}")
    print(f"null event date:              {null_event:,}")
    print(f"current join null:            {null_current:,}")
    print(f"temporal join null:           {null_temporal:,}")
    print(f"temporal multiple matches:    {temporal_multi:,}")
    print(f"SK current != temporal:       {changed:,} ({pct:.4f}%)")

    if changed:
        print("Sample changed keys:")
        (
            comparison
            .filter(
                F.col("_sk_current").isNotNull()
                & F.col("_sk_temporal").isNotNull()
                & (F.col("_sk_current") != F.col("_sk_temporal"))
            )
            .select(
                "_fact_key",
                "_id",
                "_event_ts",
                "_sk_current",
                "_sk_temporal",
            )
            .limit(10)
            .show(truncate=False)
        )

    return {
        "label": label,
        "event_date": event_date,
        "rows": total,
        "null_event": null_event,
        "null_current": null_current,
        "null_temporal": null_temporal,
        "multi": temporal_multi,
        "changed": changed,
    }


results = []

# 1) Compras: datacompra é candidata natural para produto e fornecedor.
pedidoitem = spark.table(f"{CATALOG}.silver.pedidoitem").alias("pi")
pedido = spark.table(f"{CATALOG}.silver.pedido").alias("pe")

compras = (
    pedidoitem.join(pedido, F.col("pi.id_pedido") == F.col("pe.id"), "inner")
    .select(
        F.col("pi.id").alias("id_pedidoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pe.id_fornecedor").alias("id_fornecedor"),
        F.col("pe.datacompra").alias("datacompra"),
        F.col("pe.dataentrega").alias("dataentrega"),
    )
)

results.append(
    temporal_profile(
        "fato_compras -> dim_produto",
        compras,
        "id_pedidoitem",
        "id_produto",
        "datacompra",
        DIM_PRODUTO,
        "id_produto",
        "sk_produto",
    )
)
results.append(
    temporal_profile(
        "fato_compras -> dim_fornecedor",
        compras,
        "id_pedidoitem",
        "id_fornecedor",
        "datacompra",
        DIM_FORNECEDOR,
        "id_fornecedor",
        "sk_fornecedor",
    )
)

# 2) Promoções: datainicio é candidata temporal; perfilamos sem assumir que deve vencer.
promocaoitem = spark.table(f"{CATALOG}.silver.promocaoitem").alias("pi")
promocao = spark.table(f"{CATALOG}.silver.promocao").alias("pr")

promos = (
    promocaoitem.join(promocao, F.col("pi.id_promocao") == F.col("pr.id"), "inner")
    .select(
        F.col("pi.id").alias("id_promocaoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pr.datainicio").alias("datainicio"),
        F.col("pr.datatermino").alias("datatermino"),
    )
)

results.append(
    temporal_profile(
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

# 3) Contas a pagar: comparamos três datas candidatas sem escolher por código.
parcela = spark.table(f"{CATALOG}.silver.pagarfornecedorparcela").alias("pp")
cab = spark.table(f"{CATALOG}.silver.pagarfornecedor").alias("pf")

contas = (
    parcela.join(cab, F.col("pp.id_pagarfornecedor") == F.col("pf.id"), "inner")
    .select(
        F.col("pp.id").alias("id_parcela"),
        F.col("pf.id_fornecedor").alias("id_fornecedor"),
        F.col("pf.dataemissao").alias("dataemissao"),
        F.col("pf.dataentrada").alias("dataentrada"),
        F.col("pp.datavencimento").alias("datavencimento"),
        F.col("pp.datapagamento").alias("datapagamento"),
    )
)

for dt in ["dataemissao", "dataentrada", "datavencimento"]:
    results.append(
        temporal_profile(
            "fato_contas_pagar -> dim_fornecedor",
            contas,
            "id_parcela",
            "id_fornecedor",
            dt,
            DIM_FORNECEDOR,
            "id_fornecedor",
            "sk_fornecedor",
        )
    )

# 4) Outras despesas: dataemissao e dataentrada são candidatas.
outras = (
    spark.table(f"{CATALOG}.silver.pagaroutrasdespesas")
    .select(
        F.col("id").alias("id_despesa"),
        "id_fornecedor",
        "dataemissao",
        "dataentrada",
    )
)

for dt in ["dataemissao", "dataentrada"]:
    results.append(
        temporal_profile(
            "fato_outras_despesas -> dim_fornecedor",
            outras,
            "id_despesa",
            "id_fornecedor",
            dt,
            DIM_FORNECEDOR,
            "id_fornecedor",
            "sk_fornecedor",
        )
    )

print("\n\n=== GATE G1 — TEMPORAL GOLD SUMMARY ===")
print(
    "relationship | event_date | rows | null_event | null_current | "
    "null_temporal | multi | current!=temporal"
)
for r in results:
    print(
        f"{r['label']} | {r['event_date']} | {r['rows']} | "
        f"{r['null_event']} | {r['null_current']} | {r['null_temporal']} | "
        f"{r['multi']} | {r['changed']}"
    )

bad_multi = [r for r in results if r["multi"] > 0]
if bad_multi:
    raise Exception(
        "Gate G1 encontrou múltiplas versões temporais para o mesmo evento: "
        + ", ".join(f"{r['label']}[{r['event_date']}]" for r in bad_multi)
    )

print("\n✅ G1 concluído em modo read-only.")
print("ℹ️ Diferença entre current e temporal é evidência para decisão, não falha por si só.")
