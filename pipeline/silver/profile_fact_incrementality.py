# Databricks notebook source
# pipeline/silver/profile_fact_incrementality.py
# Gate D1 — profiling read-only para desenhar Bronze -> Silver incremental dos fatos.
#
# Objetivo:
# - provar o grão por (chave, ingestion_date)
# - medir snapshots e churn de chaves
# - distinguir novos ids, ids ausentes e mudanças de payload
# - comparar cobertura Bronze histórica x Silver atual
# - NÃO altera Bronze, Silver nem watermarks

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
SNAPSHOT = "ingestion_date"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D1 só pode executar em *_dev. Recebido: {CATALOG}")

CONFIG = {
    "notaentrada":            {"chave": ["numeronota", "id_loja", "id_fornecedor"]},
    "notaentradaitem":        {"chave": ["id"]},
    "perda":                  {"chave": ["id"]},
    "logestoque":             {"chave": ["id"]},
    "promocao":               {"chave": ["id"]},
    "promocaoitem":           {"chave": ["id"]},
    "pedido":                 {"chave": ["id"]},
    "pedidoitem":             {"chave": ["id"]},
    "oferta":                 {"chave": ["id"]},
    "pagarfornecedor":        {"chave": ["id"]},
    "pagarfornecedorparcela": {"chave": ["id"]},
    "pagaroutrasdespesas":    {"chave": ["id"]},
    "pagaroutrasdespesasimposto": {"chave": ["id"]},
}


def count_null_keys(df, keys):
    cond = None
    for key in keys:
        expr = F.col(key).isNull()
        cond = expr if cond is None else (cond | expr)
    return df.filter(cond).count()


def payload_hash(df, keys):
    ignored = set(keys + [SNAPSHOT])
    payload_cols = sorted([c for c in df.columns if c not in ignored])
    if not payload_cols:
        return F.lit(0).cast("long")
    values = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in payload_cols]
    return F.xxhash64(*values)


print("\n=== GATE D1 — FACT INCREMENTALITY PROFILING ===")
print(f"Catalog: {CATALOG}")
print("Read-only: nenhuma tabela será alterada.\n")

summary = []

for tabela, cfg in CONFIG.items():
    bronze = f"{CATALOG}.bronze.{tabela}"
    silver = f"{CATALOG}.silver.{tabela}"
    keys = cfg["chave"]

    print(f"\n{'=' * 88}")
    print(f"TABLE: {tabela}")
    print(f"KEY:   {keys}")

    raw = spark.table(bronze)
    cols = set(raw.columns)
    required = set(keys + [SNAPSHOT])
    missing_cols = sorted(required - cols)
    if missing_cols:
        print(f"❌ Colunas obrigatórias ausentes: {missing_cols}")
        summary.append((tabela, "INVALID_SCHEMA", None, None, None, None, None, None))
        continue

    total_rows = raw.count()
    null_keys = count_null_keys(raw, keys)
    snapshot_stats = (
        raw.agg(
            F.countDistinct(SNAPSHOT).alias("snapshots"),
            F.min(SNAPSHOT).alias("min_snapshot"),
            F.max(SNAPSHOT).alias("max_snapshot"),
        ).collect()[0]
    )

    snapshots = snapshot_stats["snapshots"]
    min_snapshot = snapshot_stats["min_snapshot"]
    max_snapshot = snapshot_stats["max_snapshot"]

    dup_groups = (
        raw.groupBy(*(keys + [SNAPSHOT]))
           .count()
           .filter(F.col("count") > 1)
           .count()
    )

    all_distinct_keys = raw.select(*keys).distinct().count()

    recent = (
        raw.select(SNAPSHOT)
           .where(F.col(SNAPSHOT).isNotNull())
           .distinct()
           .orderBy(F.col(SNAPSHOT).desc())
           .limit(2)
           .collect()
    )
    latest = recent[0][SNAPSHOT] if recent else None
    previous = recent[1][SNAPSHOT] if len(recent) > 1 else None

    latest_rows = None
    latest_keys_count = None
    previous_rows = None
    previous_keys_count = None
    added_keys = None
    missing_keys = None
    changed_shared = None
    unchanged_shared = None

    if latest is not None:
        latest_df = raw.filter(F.col(SNAPSHOT) == F.lit(latest))
        latest_rows = latest_df.count()
        latest_keys = latest_df.select(*keys).distinct()
        latest_keys_count = latest_keys.count()

        if previous is not None:
            previous_df = raw.filter(F.col(SNAPSHOT) == F.lit(previous))
            previous_rows = previous_df.count()
            previous_keys = previous_df.select(*keys).distinct()
            previous_keys_count = previous_keys.count()

            added_keys = latest_keys.join(previous_keys, on=keys, how="left_anti").count()
            missing_keys = previous_keys.join(latest_keys, on=keys, how="left_anti").count()

            # Só atribuímos mudança de payload se o grão por snapshot for íntegro.
            if dup_groups == 0:
                latest_h = latest_df.select(
                    *keys, payload_hash(latest_df, keys).alias("_payload_hash")
                )
                previous_h = previous_df.select(
                    *keys, payload_hash(previous_df, keys).alias("_prev_payload_hash")
                )
                shared = latest_h.join(previous_h, on=keys, how="inner")
                changed_shared = shared.filter(
                    F.col("_payload_hash") != F.col("_prev_payload_hash")
                ).count()
                unchanged_shared = shared.filter(
                    F.col("_payload_hash") == F.col("_prev_payload_hash")
                ).count()

    # Mudanças históricas observadas por chave ao longo dos snapshots.
    change_events = None
    changed_ids = None
    max_snapshots_per_key = None
    if dup_groups == 0:
        hist = raw.select(
            *keys,
            F.col(SNAPSHOT),
            payload_hash(raw, keys).alias("_payload_hash"),
        )
        w = Window.partitionBy(*keys).orderBy(F.col(SNAPSHOT).asc())
        hist = (
            hist.withColumn("_prev_hash", F.lag("_payload_hash").over(w))
                .withColumn("_prev_snapshot", F.lag(SNAPSHOT).over(w))
        )
        changes = hist.filter(
            F.col("_prev_snapshot").isNotNull()
            & (F.col("_payload_hash") != F.col("_prev_hash"))
        )
        change_events = changes.count()
        changed_ids = changes.select(*keys).distinct().count()
        max_snapshots_per_key = (
            hist.groupBy(*keys).count().agg(F.max("count")).collect()[0][0]
        )

    silver_rows = None
    silver_keys = None
    silver_missing_vs_bronze = None
    silver_extra_vs_bronze = None
    if spark.catalog.tableExists(silver):
        silver_df = spark.table(silver)
        silver_rows = silver_df.count()
        silver_key_df = silver_df.select(*keys).distinct()
        silver_keys = silver_key_df.count()
        bronze_key_df = raw.select(*keys).distinct()
        silver_missing_vs_bronze = bronze_key_df.join(
            silver_key_df, on=keys, how="left_anti"
        ).count()
        silver_extra_vs_bronze = silver_key_df.join(
            bronze_key_df, on=keys, how="left_anti"
        ).count()

    print(f"Bronze rows:                   {total_rows:,}")
    print(f"Snapshots:                     {snapshots} | {min_snapshot} -> {max_snapshot}")
    print(f"Distinct keys historical:      {all_distinct_keys:,}")
    print(f"Null-key rows:                 {null_keys:,}")
    print(f"Duplicate key/snapshot groups: {dup_groups:,}")
    print(f"Latest snapshot:               {latest} | rows={latest_rows:,} | keys={latest_keys_count:,}" if latest is not None else "Latest snapshot: n/a")
    if previous is not None:
        print(f"Previous snapshot:             {previous} | rows={previous_rows:,} | keys={previous_keys_count:,}")
        print(f"Latest vs previous:            added={added_keys:,} | missing={missing_keys:,}")
        if changed_shared is not None:
            print(f"Shared-key payload:             changed={changed_shared:,} | unchanged={unchanged_shared:,}")
    if change_events is not None:
        print(f"Historical payload changes:    events={change_events:,} | changed keys={changed_ids:,}")
        print(f"Max snapshots per key:         {max_snapshots_per_key:,}")
    else:
        print("Historical payload changes:    n/a (duplicate key/snapshot requires modeling decision)")

    if silver_rows is not None:
        print(f"Silver current:                rows={silver_rows:,} | distinct keys={silver_keys:,}")
        print(f"Bronze↔Silver key coverage:    missing_in_silver={silver_missing_vs_bronze:,} | extra_in_silver={silver_extra_vs_bronze:,}")

    summary.append((
        tabela,
        "OK" if null_keys == 0 and dup_groups == 0 else "REVIEW",
        snapshots,
        total_rows,
        all_distinct_keys,
        null_keys,
        dup_groups,
        change_events,
    ))

print("\n\n=== GATE D1 — SUMMARY ===")
print("table | status | snapshots | bronze_rows | historical_keys | null_keys | dup_key_snapshot | change_events")
for row in summary:
    print(" | ".join("n/a" if v is None else str(v) for v in row))

review = [r[0] for r in summary if r[1] != "OK"]
if review:
    print(f"\n⚠️ Tabelas que exigem decisão antes do runtime incremental: {review}")
else:
    print("\n✅ Todas as tabelas possuem key/snapshot íntegro para avançar ao desenho do watermark.")

print("\nGate D1 é diagnóstico. Nenhum watermark foi criado ou avançado.")
