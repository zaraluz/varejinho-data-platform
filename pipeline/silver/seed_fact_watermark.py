# Databricks notebook source
# pipeline/silver/seed_fact_watermark.py
# Gate D3 — cria/semeia o estado de controle para Bronze -> Silver incremental.
#
# Pré-condição operacional: Gate D2 aprovado.
# Política de seed:
# - committed = max(ingestion_date) já materializado na Silver atual
# - nunca avança para além do que a Silver realmente contém
# - qualquer snapshot Bronze posterior permanece pendente
# - nunca reseta/retrocede watermark já existente

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_TABLE = job_param(
    "control_table",
    f"{CATALOG}.control.fact_watermark",
)

FACTS = [
    "notaentrada",
    "notaentradaitem",
    "perda",
    "logestoque",
    "promocao",
    "promocaoitem",
    "pedido",
    "pedidoitem",
    "oferta",
    "pagarfornecedor",
    "pagarfornecedorparcela",
    "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto",
]

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate D3 só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.control")

if not spark.catalog.tableExists(CONTROL_TABLE):
    spark.sql(f"""
        CREATE TABLE {CONTROL_TABLE} (
            entity STRING NOT NULL,
            last_processed_snapshot DATE,
            candidate_snapshot DATE,
            status STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING DELTA
    """)
    print(f"✅ Tabela de controle criada: {CONTROL_TABLE}")
else:
    print(f"ℹ️ Tabela de controle já existe: {CONTROL_TABLE}")

print("\n=== GATE D3 — SEED FACT WATERMARK ===")
print(f"Catalog: {CATALOG}")
print(f"Control: {CONTROL_TABLE}")
print("Seed usa o maior ingestion_date JÁ presente na Silver, nunca o max da Bronze.\n")

for entity in FACTS:
    bronze = f"{CATALOG}.bronze.{entity}"
    silver = f"{CATALOG}.silver.{entity}"

    if not spark.catalog.tableExists(bronze):
        raise Exception(f"Bronze ausente: {bronze}")
    if not spark.catalog.tableExists(silver):
        raise Exception(f"Silver ausente: {silver}")

    bronze_max = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]
    silver_max = spark.table(silver).agg(F.max("ingestion_date")).collect()[0][0]

    if bronze_max is None:
        raise Exception(f"{entity}: Bronze sem ingestion_date válido")
    if silver_max is None:
        raise Exception(f"{entity}: Silver sem ingestion_date válido")
    if silver_max > bronze_max:
        raise Exception(
            f"{entity}: Silver max {silver_max} está à frente da Bronze max {bronze_max}"
        )

    existing = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )

    if len(existing) > 1:
        raise Exception(
            f"{entity}: esperado no máximo 1 watermark; encontrado={len(existing)}"
        )

    if len(existing) == 1:
        row = existing[0]
        committed = row["last_processed_snapshot"]
        candidate = row["candidate_snapshot"]
        status = row["status"]

        if status != "COMMITTED" or candidate is not None:
            raise Exception(
                f"{entity}: watermark já existe em estado não-sementeável: "
                f"committed={committed}, candidate={candidate}, status={status}"
            )

        if committed is not None and committed > bronze_max:
            raise Exception(
                f"{entity}: committed {committed} está à frente da Bronze {bronze_max}"
            )

        print(
            f"ℹ️ {entity}: watermark já existe; preservado sem reset "
            f"(committed={committed}, Bronze max={bronze_max})"
        )
        continue

    spark.sql(f"""
        INSERT INTO {CONTROL_TABLE}
        VALUES (
            '{entity}',
            DATE '{silver_max}',
            NULL,
            'COMMITTED',
            current_timestamp()
        )
    """)

    pending_snapshots = (
        spark.table(bronze)
        .filter(F.col("ingestion_date") > F.lit(silver_max))
        .select("ingestion_date")
        .distinct()
        .count()
    )

    print(
        f"✅ {entity}: committed={silver_max} | Bronze max={bronze_max} "
        f"| snapshots pendentes={pending_snapshots}"
    )

print("\n=== ESTADO FINAL ===")
state = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity").isin(FACTS))
    .orderBy("entity")
)

rows = state.collect()
if len(rows) != len(FACTS):
    raise Exception(
        f"Esperadas {len(FACTS)} linhas de controle; encontrado={len(rows)}"
    )

for row in rows:
    if row["status"] != "COMMITTED" or row["candidate_snapshot"] is not None:
        raise Exception(
            f"Estado inválido após seed para {row['entity']}: "
            f"status={row['status']} candidate={row['candidate_snapshot']}"
        )
    print(
        f"{row['entity']}: committed={row['last_processed_snapshot']} "
        f"| candidate={row['candidate_snapshot']} | status={row['status']}"
    )

print(
    f"\n✅ Gate D3 concluído: {len(rows)}/{len(FACTS)} watermarks em estado COMMITTED."
)
print("✅ Nenhum watermark existente foi resetado ou avançado.")
print("✅ Próximo passo: provar o runtime incremental em sandbox antes de ligar o pipeline oficial.")
