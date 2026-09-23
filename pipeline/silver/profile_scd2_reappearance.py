# Databricks notebook source
# pipeline/silver/profile_scd2_reappearance.py
# R3 read-only profile: IDs that disappear from one source snapshot and later reappear.
#
# Reappearance semantics measured here:
# - current observation is not the first observation of the ID
# - ID was absent from the immediately previous available source snapshot
# - compare current state to the ID's last observed state, matching full-backfill LAG semantics

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")

if not CATALOG.endswith("_dev"):
    raise Exception(f"R3 profiler is dev-only. Received: {CATALOG}")

CONFIG = {
    "produto": {
        "type2": [
            "descricaocompleta",
            "mercadologico1",
            "mercadologico2",
            "mercadologico3",
            "ncm1",
            "id_tipoembalagem",
        ],
        "type1": [
            "descricaoreduzida",
            "id_tipomercadoria",
            "pesoliquido",
            "pesobruto",
        ],
    },
    "fornecedor": {
        "type2": ["cnpj", "razaosocial"],
        "type1": [
            "nomefantasia",
            "id_situacaocadastro",
            "id_tipoempresa",
            "permitenfsempedido",
            "id_tipocustocompra",
            "id_tipocustodevolucaotroca",
            "pedidominimoqtd",
            "pedidominimovalor",
            "valormaximoverbapedido",
            "id_contacontabilfinanceiro",
            "id_fornecedorfavorecido",
            "id_municipio",
        ],
    },
    "mercadologico": {
        "type2": [
            "mercadologico1",
            "mercadologico2",
            "mercadologico3",
            "mercadologico4",
            "mercadologico5",
            "nivel",
        ],
        "type1": ["descricao"],
    },
}


def hash_expr(cols):
    return F.md5(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
                for c in cols
            ],
        )
    )


def any_changed(current_cols, previous_prefix="_prev_t1_"):
    expr = None
    for col in current_cols:
        changed = ~F.col(col).eqNullSafe(F.col(f"{previous_prefix}{col}"))
        expr = changed if expr is None else (expr | changed)
    return expr if expr is not None else F.lit(False)


summary = []

print("\n=== R3 — SCD2 REAPPEARANCE PROFILE [READ ONLY] ===")
print(f"Bronze source catalog: {BRONZE_SOURCE_CATALOG}\n")

for entity, cfg in CONFIG.items():
    table = f"{BRONZE_SOURCE_CATALOG}.bronze.{entity}"
    if not spark.catalog.tableExists(table):
        raise Exception(f"Missing Bronze source: {table}")

    raw = spark.table(table)
    required = {"id", "ingestion_date", *cfg["type2"], *cfg["type1"]}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise Exception(f"{entity}: required columns missing: {missing}")

    duplicate_groups = (
        raw.groupBy("id", "ingestion_date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_groups:
        raise Exception(
            f"{entity}: {duplicate_groups} duplicate group(s) by (id, ingestion_date)"
        )

    snapshots = (
        raw.select("ingestion_date")
        .where(F.col("ingestion_date").isNotNull())
        .distinct()
    )
    w_snap = Window.orderBy("ingestion_date")
    snapshot_calendar = snapshots.withColumn(
        "_previous_global_snapshot",
        F.lag("ingestion_date").over(w_snap),
    )

    base = raw.withColumn("_type2_hash", hash_expr(cfg["type2"]))
    w_id = Window.partitionBy("id").orderBy(F.col("ingestion_date").asc())

    observed = (
        base.withColumn(
            "_previous_observed_snapshot",
            F.lag("ingestion_date").over(w_id),
        )
        .withColumn(
            "_previous_type2_hash",
            F.lag("_type2_hash").over(w_id),
        )
    )

    for col in cfg["type1"]:
        observed = observed.withColumn(
            f"_prev_t1_{col}",
            F.lag(F.col(col)).over(w_id),
        )

    classified = observed.join(
        snapshot_calendar,
        on="ingestion_date",
        how="left",
    )

    reappeared = classified.filter(
        F.col("_previous_observed_snapshot").isNotNull()
        & F.col("_previous_global_snapshot").isNotNull()
        & (
            F.col("_previous_observed_snapshot")
            < F.col("_previous_global_snapshot")
        )
    )

    total = reappeared.count()
    ids = reappeared.select("id").distinct().count()

    same_type2 = reappeared.filter(
        F.col("_type2_hash").eqNullSafe(F.col("_previous_type2_hash"))
    ).count()
    changed_type2 = reappeared.filter(
        ~F.col("_type2_hash").eqNullSafe(F.col("_previous_type2_hash"))
    ).count()

    t1_changed = reappeared.filter(
        any_changed(cfg["type1"])
    ).count()

    latest = (
        reappeared.agg(F.max("ingestion_date").alias("latest"))
        .collect()[0]["latest"]
        if total
        else None
    )

    max_gap = (
        reappeared.select(
            F.datediff(
                F.col("ingestion_date"),
                F.col("_previous_observed_snapshot"),
            ).alias("_gap_days")
        )
        .agg(F.max("_gap_days").alias("max_gap"))
        .collect()[0]["max_gap"]
        if total
        else None
    )

    summary.append(
        {
            "entity": entity,
            "reappearance_rows": total,
            "distinct_ids": ids,
            "same_type2": same_type2,
            "changed_type2": changed_type2,
            "type1_changed": t1_changed,
            "latest_reappearance": latest,
            "max_observation_gap_days": max_gap,
        }
    )

    print(f"--- {entity} ---")
    print(f"reappearance observations: {total:,}")
    print(f"distinct IDs:              {ids:,}")
    print(f"same Type 2 state:         {same_type2:,}")
    print(f"changed Type 2 state:      {changed_type2:,}")
    print(f"Type 1 changed:            {t1_changed:,}")
    print(f"latest reappearance:       {latest}")
    print(f"max observed gap days:     {max_gap}")

    if total:
        print("sample:")
        (
            reappeared.select(
                "id",
                "_previous_observed_snapshot",
                "_previous_global_snapshot",
                "ingestion_date",
                F.when(
                    F.col("_type2_hash").eqNullSafe(
                        F.col("_previous_type2_hash")
                    ),
                    F.lit("SAME_TYPE2"),
                )
                .otherwise(F.lit("CHANGED_TYPE2"))
                .alias("classification"),
            )
            .orderBy(F.col("ingestion_date").desc(), "id")
            .limit(10)
            .show(truncate=False)
        )
    print()

summary_schema = StructType(
    [
        StructField("entity", StringType(), False),
        StructField("reappearance_rows", LongType(), False),
        StructField("distinct_ids", LongType(), False),
        StructField("same_type2", LongType(), False),
        StructField("changed_type2", LongType(), False),
        StructField("type1_changed", LongType(), False),
        StructField("latest_reappearance", DateType(), True),
        StructField("max_observation_gap_days", IntegerType(), True),
    ]
)

summary_df = spark.createDataFrame(summary, schema=summary_schema)
print("=== R3 FINAL SUMMARY ===")
summary_df.orderBy("entity").show(truncate=False)

print("✅ Read-only profile complete. No Silver/control state was modified.")
