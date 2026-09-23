# Databricks notebook source
# ops/repair/verify_fact_maturity_repair.py
# Gate D6B — verificação final do repair real das 13 facts.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")

FACTS = [
    "notaentrada", "notaentradaitem", "perda", "logestoque",
    "promocao", "promocaoitem", "pedido", "pedidoitem", "oferta",
    "pagarfornecedor", "pagarfornecedorparcela",
    "pagaroutrasdespesas", "pagaroutrasdespesasimposto",
]

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D6B só pode executar em *_dev. Recebido: {CATALOG}")

checks = []

for entity in FACTS:
    source = f"{BRONZE_SOURCE_CATALOG}.bronze.{entity}"
    silver = f"{CATALOG}.silver.{entity}"

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

    rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )

    if len(rows) != 1:
        ok = False
        detail = f"watermark rows={len(rows)}"
    else:
        state = rows[0]
        future = (
            spark.table(silver)
            .filter(F.col("ingestion_date") > F.lit(mature_cutoff))
            .count()
        )

        ok = (
            state["status"] == "COMMITTED"
            and state["candidate_snapshot"] is None
            and state["last_processed_snapshot"] == mature_cutoff
            and future == 0
        )

        detail = (
            f"committed={state['last_processed_snapshot']} | "
            f"mature_cutoff={mature_cutoff} | "
            f"candidate={state['candidate_snapshot']} | "
            f"status={state['status']} | future_rows={future}"
        )

    checks.append((entity, ok, detail))
    print(f"{'✅' if ok else '❌'} {entity} — {detail}")

passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)

print(f"\n=== RESULTADO D6B: {passed}/{total} facts reparadas ===")

if passed != total:
    failed = [entity for entity, ok, _ in checks if not ok]
    raise Exception(
        "Gate D6B final falhou; entidades pendentes: " + ", ".join(failed)
    )

print("✅ 13/13 watermarks alinhados ao último mature_cutoff.")
print("✅ candidate_snapshot limpo em todas as entidades.")
print("✅ Nenhuma Silver contém linha acima da partição madura.")
print("✅ Baseline real está pronto para retomar o runtime incremental D+1.")
