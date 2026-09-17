# Databricks notebook source
# pipeline/silver/prepare_scd2_product_replay.py
# Gate B6 — prepara replay isolado do último snapshot para provar idempotência.
# Não altera varejinho_dev.silver.produto nem o watermark real.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = f"{CATALOG}.bronze.produto"
SOURCE_SILVER = f"{CATALOG}.silver.produto"
REAL_CONTROL = f"{CATALOG}.control.scd2_watermark"
REPLAY_SILVER = f"{CATALOG}.silver._b6_produto_replay"
BASELINE_SILVER = f"{CATALOG}.silver._b6_produto_replay_baseline"
REPLAY_CONTROL = f"{CATALOG}.control._b6_scd2_watermark"
ENTITY = "produto"
SNAPSHOT = "ingestion_date"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B6 só pode executar em *_dev. Recebido: {CATALOG}")

for required_table in [BRONZE, SOURCE_SILVER, REAL_CONTROL]:
    if not spark.catalog.tableExists(required_table):
        raise Exception(f"Pré-requisito ausente: {required_table}")

snapshot_dates = [
    r[SNAPSHOT]
    for r in spark.table(BRONZE).select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()
]
if len(snapshot_dates) < 2:
    raise Exception("Gate B6 requer pelo menos 2 snapshots Bronze")

replay_snapshot = snapshot_dates[-1]
previous_snapshot = snapshot_dates[-2]

real_rows = spark.table(REAL_CONTROL).filter(F.col("entity") == ENTITY).collect()
if len(real_rows) != 1:
    raise Exception(f"Esperada exatamente 1 linha no watermark real de {ENTITY}; encontrado={len(real_rows)}")

real = real_rows[0]
if (
    real["last_processed_snapshot"] != replay_snapshot
    or real["candidate_snapshot"] is not None
    or real["status"] != "COMMITTED"
):
    raise Exception(
        "Estado real não está pronto para replay: "
        f"committed={real['last_processed_snapshot']}, candidate={real['candidate_snapshot']}, "
        f"status={real['status']}, Bronze max={replay_snapshot}"
    )

# Descobre o que o último delta Bronze contém; isso prova que o replay exercita
# cenários reais, não apenas um no-op vazio.
current = spark.table(BRONZE).filter(F.col(SNAPSHOT) == F.lit(replay_snapshot))
previous = spark.table(BRONZE).filter(F.col(SNAPSHOT) == F.lit(previous_snapshot))
new_ids = current.select("id").join(previous.select("id"), on="id", how="left_anti").count()
disappeared_ids = previous.select("id").join(current.select("id"), on="id", how="left_anti").count()

# Remove qualquer resíduo de uma execução B6 anterior bem ou mal sucedida.
for table_name in [REPLAY_CONTROL, REPLAY_SILVER, BASELINE_SILVER]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Duas cópias físicas pequenas da Silver dev:
# - baseline = verdade antes do replay
# - replay   = alvo onde o mesmo engine incremental será executado
spark.sql(f"CREATE TABLE {BASELINE_SILVER} USING DELTA AS SELECT * FROM {SOURCE_SILVER}")
spark.sql(f"CREATE TABLE {REPLAY_SILVER} USING DELTA AS SELECT * FROM {SOURCE_SILVER}")

spark.sql(f"""
    CREATE TABLE {REPLAY_CONTROL} (
        entity STRING,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING,
        updated_at TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {REPLAY_CONTROL}
    VALUES ('{ENTITY}', DATE '{previous_snapshot}', NULL, 'COMMITTED', current_timestamp())
""")

baseline = spark.table(BASELINE_SILVER)
source = spark.table(SOURCE_SILVER)

baseline_rows = baseline.count()
source_rows = source.count()
baseline_ids = baseline.select("id").distinct().count()
versioned_ids = baseline.groupBy("id").count().filter(F.col("count") > 1).count()

if baseline_rows != source_rows:
    raise Exception(f"Cópia baseline divergente da Silver real: baseline={baseline_rows}, source={source_rows}")

print("\n=== GATE B6 — PREPARE CONTROLLED REPLAY ===")
print(f"Bronze:          {BRONZE}")
print(f"Silver real:     {SOURCE_SILVER}")
print(f"Silver replay:   {REPLAY_SILVER}")
print(f"Baseline replay: {BASELINE_SILVER}")
print(f"Control replay:  {REPLAY_CONTROL}\n")
print(f"snapshot anterior:       {previous_snapshot}")
print(f"snapshot a reprocessar:  {replay_snapshot}")
print(f"novos ids no último delta Bronze: {new_ids:,}")
print(f"ids que sumiram no último delta:   {disappeared_ids:,}")
print(f"baseline versões:         {baseline_rows:,}")
print(f"baseline ids:             {baseline_ids:,}")
print(f"baseline ids versionados: {versioned_ids:,}")
print("\n✅ Sandbox B6 preparada. O watermark REAL e a Silver REAL não foram alterados.")
print(f"O engine incremental agora deve reprocessar {replay_snapshot} sobre a cópia e terminar sem mudar nenhuma linha.")
