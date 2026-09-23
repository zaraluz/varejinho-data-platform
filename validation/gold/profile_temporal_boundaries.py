# Databricks notebook source
# validation/gold/profile_temporal_boundaries.py
# Gate G2 — auditoria read-only dos eventos que caem antes da primeira versão SCD2.
#
# Objetivo:
# - NÃO alterar Silver/Gold.
# - Explicar os before_first encontrados no G1.
# - Distinguir boundary sustentado por datacadastro de boundary limitado ao primeiro snapshot observado.
# - Produzir evidência para a política de fallback temporal; não escolher fallback automaticamente.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate G2 só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
    )


def first_versions(table_name: str):
    dim = spark.table(f"{CATALOG}.silver.{table_name}")
    required = {
        "id",
        "valid_from",
        "valid_from_source",
        "scd_source_snapshot",
        "datacadastro",
    }
    missing = sorted(required - set(dim.columns))
    if missing:
        raise Exception(
            f"{CATALOG}.silver.{table_name} não possui metadados necessários ao G2: {missing}"
        )

    w = Window.partitionBy("id").orderBy(
        F.col("valid_from").asc(),
        F.col("scd_source_snapshot").asc(),
    )

    return (
        dim.withColumn("_rn_first", F.row_number().over(w))
        .filter(F.col("_rn_first") == 1)
        .select(
            F.col("id").alias("_id"),
            F.col("valid_from").alias("_first_valid_from"),
            F.col("valid_from_source").alias("_valid_from_source"),
            F.col("scd_source_snapshot").alias("_scd_source_snapshot"),
            F.col("datacadastro").alias("_datacadastro_raw"),
        )
    )


FIRST_PRODUTO = first_versions("produto")
FIRST_FORNECEDOR = first_versions("fornecedor")


def profile_before_first(
    label,
    facts,
    fact_key,
    fact_natural_key,
    event_date,
    first_dim,
):
    print(f"\n{'=' * 100}")
    print(f"{label} | event_date={event_date}")

    base = (
        facts.select(
            F.col(fact_key).alias("_fact_key"),
            F.col(fact_natural_key).alias("_id"),
            F.col(event_date).alias("_event_raw"),
        )
        .withColumn("_event_ts", F.expr("try_cast(_event_raw as timestamp)"))
    )

    joined = base.join(first_dim, on="_id", how="left")

    missing_dim = joined.filter(F.col("_first_valid_from").isNull()).count()
    unparseable = joined.filter(
        F.col("_event_raw").isNotNull() & F.col("_event_ts").isNull()
    ).count()

    before = (
        joined.filter(
            F.col("_event_ts").isNotNull()
            & F.col("_first_valid_from").isNotNull()
            & (F.col("_event_ts") < F.col("_first_valid_from"))
        )
        .withColumn(
            "_days_before",
            F.datediff(
                F.to_date(F.col("_first_valid_from")),
                F.to_date(F.col("_event_ts")),
            ),
        )
    )

    rows = before.count()
    ids = before.select("_id").distinct().count()

    source_counts = {
        (r["_valid_from_source"] if r["_valid_from_source"] is not None else "<NULL>"): r["count"]
        for r in before.groupBy("_valid_from_source").count().collect()
    }

    stats = before.agg(
        F.min("_event_ts").alias("min_event"),
        F.max("_event_ts").alias("max_event"),
        F.min("_days_before").alias("min_days_before"),
        F.expr("percentile_approx(_days_before, 0.5)").alias("median_days_before"),
        F.max("_days_before").alias("max_days_before"),
    ).collect()[0]

    print(f"before_first rows:              {rows:,}")
    print(f"distinct dimension ids:         {ids:,}")
    print(f"missing first dimension row:    {missing_dim:,}")
    print(f"unparseable event date:         {unparseable:,}")
    print(f"valid_from_source distribution: {source_counts}")

    if rows:
        print(f"event range:                    {stats['min_event']} -> {stats['max_event']}")
        print(
            "days before first version:      "
            f"min={stats['min_days_before']} | "
            f"median={stats['median_days_before']} | "
            f"max={stats['max_days_before']}"
        )

        print("Top boundary groups:")
        (
            before.groupBy(
                "_id",
                "_first_valid_from",
                "_valid_from_source",
                "_scd_source_snapshot",
                "_datacadastro_raw",
            )
            .agg(
                F.count("*").alias("event_count"),
                F.min("_event_ts").alias("first_event"),
                F.max("_event_ts").alias("last_event"),
                F.min("_days_before").alias("min_days_before"),
                F.max("_days_before").alias("max_days_before"),
            )
            .orderBy(
                F.col("event_count").desc(),
                F.col("max_days_before").desc(),
                F.col("_id").asc(),
            )
            .limit(25)
            .show(truncate=False)
        )

    return {
        "label": label,
        "event_date": event_date,
        "rows": rows,
        "ids": ids,
        "missing_dim": missing_dim,
        "unparseable": unparseable,
        "source_counts": source_counts,
        "min_days_before": stats["min_days_before"] if rows else None,
        "median_days_before": stats["median_days_before"] if rows else None,
        "max_days_before": stats["max_days_before"] if rows else None,
    }


results = []

# Compras -> produto | semântica candidata aprovada para auditoria: datacompra.
pedidoitem = spark.table(f"{CATALOG}.silver.pedidoitem").alias("pi")
pedido = spark.table(f"{CATALOG}.silver.pedido").alias("pe")
compras = (
    pedidoitem.join(pedido, F.col("pi.id_pedido") == F.col("pe.id"), "inner")
    .select(
        F.col("pi.id").alias("id_pedidoitem"),
        F.col("pi.id_produto").alias("id_produto"),
        F.col("pe.datacompra").alias("datacompra"),
    )
)
results.append(
    profile_before_first(
        "fato_compras -> dim_produto",
        compras,
        "id_pedidoitem",
        "id_produto",
        "datacompra",
        FIRST_PRODUTO,
    )
)

# Promoções -> produto | semântica candidata: início da promoção.
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
    profile_before_first(
        "fato_promocoes -> dim_produto",
        promos,
        "id_promocaoitem",
        "id_produto",
        "datainicio",
        FIRST_PRODUTO,
    )
)

# Contas a pagar -> fornecedor | emissão representa o nascimento do documento/obrigação.
parcela = spark.table(f"{CATALOG}.silver.pagarfornecedorparcela").alias("pp")
cab = spark.table(f"{CATALOG}.silver.pagarfornecedor").alias("pf")
contas = (
    parcela.join(cab, F.col("pp.id_pagarfornecedor") == F.col("pf.id"), "inner")
    .select(
        F.col("pp.id").alias("id_parcela"),
        F.col("pf.id_fornecedor").alias("id_fornecedor"),
        F.col("pf.dataemissao").alias("dataemissao"),
    )
)
results.append(
    profile_before_first(
        "fato_contas_pagar -> dim_fornecedor",
        contas,
        "id_parcela",
        "id_fornecedor",
        "dataemissao",
        FIRST_FORNECEDOR,
    )
)

# Outras despesas -> fornecedor | emissão já define o tempo do fato na Gold.
outras = (
    spark.table(f"{CATALOG}.silver.pagaroutrasdespesas")
    .select(
        F.col("id").alias("id_despesa"),
        "id_fornecedor",
        "dataemissao",
    )
)
results.append(
    profile_before_first(
        "fato_outras_despesas -> dim_fornecedor",
        outras,
        "id_despesa",
        "id_fornecedor",
        "dataemissao",
        FIRST_FORNECEDOR,
    )
)

print("\n\n=== GATE G2 — BEFORE-FIRST BOUNDARY SUMMARY ===")
print(
    "relationship | event_date | rows | ids | missing_dim | unparseable | "
    "source_counts | min_days | median_days | max_days"
)
for r in results:
    print(
        f"{r['label']} | {r['event_date']} | {r['rows']} | {r['ids']} | "
        f"{r['missing_dim']} | {r['unparseable']} | {r['source_counts']} | "
        f"{r['min_days_before']} | {r['median_days_before']} | {r['max_days_before']}"
    )

unexpected = [
    r for r in results
    if r["missing_dim"] > 0 or r["unparseable"] > 0
]
if unexpected:
    raise Exception(
        "Gate G2 encontrou condição inesperada nas relações escolhidas: "
        + ", ".join(
            f"{r['label']}[missing_dim={r['missing_dim']}, unparseable={r['unparseable']}]"
            for r in unexpected
        )
    )

print("\n✅ G2 concluído em modo read-only.")
print("ℹ️ valid_from_source=datacadastro e ingestion_date exigem políticas diferentes; o G2 não aplica fallback.")
