# Databricks notebook source
# pipeline/silver/prepare_scd2_replay.py
# Gate B7G — prepara replay genérico isolado do último snapshot para provar idempotência.
# Não altera a Silver real nem o watermark real.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "fornecedor")
BRONZE = job_param("bronze_table", f"{CATALOG}.bronze.{ENTITY}")
SOURCE_SILVER = job_param("silver_table", f"{CATALOG}.silver.{ENTITY}")
REAL_CONTROL = job_param("control_table", f"{CATALOG}.control.scd2_watermark")
REPLAY_SILVER = job_param("replay_silver", f"{CATALOG}.silver._replay_{ENTITY}")
BASELINE_SILVER = job_param("baseline_silver", f"{CATALOG}.silver._replay_{ENTITY}_baseline")
REPLAY_CONTROL = job_param("replay_control", f"{CATALOG}.control._replay_{ENTITY}_watermark")
SNAPSHOT = "ingestion_date"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Replay SCD2 só pode executar em *_dev durante hardening. Recebido: {CATALOG}")

for required_table in [BRONZE, SOURCE_SILVER, REAL_CONTROL]:
    if not spark.catalog.tableExists(required_table):
        raise Exception(f"Pré-requisito ausente: {required_table}")

snapshot_dates = [
    r[SNAPSHOT]
    for r in spark.table(BRONZE).select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()
]
if len(snapshot_dates) < 2:
    raise Exception("Replay requer pelo menos 2 snapshots Bronze")

replay_snapshot = snapshot_dates[-1]
previous_snapshot = snapshot_dates[-2]

real_rows = spark.table(REAL_CONTROL).filter(F.col("entity") == ENTITY).collect()
if len(real_rows) != 1:
    raise Exception(
        f"Esperada exatamente 1 linha no watermark real de {ENTITY}; encontrado={len(real_rows)}"
    )

real = real_rows[0]
if (
    real["last_processed_snapshot"] != replay_snapshot
    or real["candidate_snapshot"] is not None
    or real["status"] != "COMMITTED"
):
    raise Exception(
        "Estado real não está pronto para replay: "
        f"entity={ENTITY}, committed={real['last_processed_snapshot']}, "
        f"candidate={real['candidate_snapshot']}, status={real['status']}, "
        f"Bronze max={replay_snapshot}"
    )

current = spark.table(BRONZE).filter(F.col(SNAPSHOT) == F.lit(replay_snapshot))
previous = spark.table(BRONZE).filter(F.col(SNAPSHOT) == F.lit(previous_snapshot))

new_ids = current.select("id").join(previous.select("id"), on="id", how="left_anti").count()
disappeared_ids = previous.select("id").join(current.select("id"), on="id", how="left_anti").count()

# Limpa resíduos de uma execução anterior.
for table_name in [REPLAY_CONTROL, REPLAY_SILVER, BASELINE_SILVER]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# baseline = fotografia imutável; replay = alvo do engine incremental.
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

# Antes do teste, a cópia precisa ser fiel à Silver real.
missing = source.exceptAll(baseline).count()
unexpected = baseline.exceptAll(source).count()
if source_rows != baseline_rows or missing or unexpected:
    raise Exception(
        "Baseline replay não é cópia exata da Silver real: "
        f"rows source={source_rows}, baseline={baseline_rows}, "
        f"missing={missing}, unexpected={unexpected}"
    )

print("\n=== GATE B7G — PREPARE GENERIC CONTROLLED REPLAY ===")
print(f"Entity:           {ENTITY}")
print(f"Bronze:           {BRONZE}")
print(f"Silver real:      {SOURCE_SILVER}")
print(f"Silver replay:    {REPLAY_SILVER}")
print(f"Baseline replay:  {BASELINE_SILVER}")
print(f"Control replay:   {REPLAY_CONTROL}\n")
print(f"snapshot anterior:       {previous_snapshot}")
print(f"snapshot a reprocessar:  {replay_snapshot}")
print(f"novos ids no último delta Bronze: {new_ids:,}")
print(f"ids que sumiram no último delta:   {disappeared_ids:,}")
print(f"baseline versões:         {baseline_rows:,}")
print(f"baseline ids:             {baseline_ids:,}")
print(f"baseline ids versionados: {versioned_ids:,}")
print("\n✅ Sandbox de replay preparada.")
print("✅ Silver real e watermark real permanecem intocados.")
