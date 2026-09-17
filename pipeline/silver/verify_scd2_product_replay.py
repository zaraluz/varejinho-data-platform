# Databricks notebook source
# pipeline/silver/verify_scd2_product_replay.py
# Gate B6 — prova idempotência por igualdade exata antes/depois do replay.
# Só limpa as tabelas sandbox se todos os checks passarem.

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

for required_table in [BRONZE, SOURCE_SILVER, REAL_CONTROL, REPLAY_SILVER, BASELINE_SILVER, REPLAY_CONTROL]:
    if not spark.catalog.tableExists(required_table):
        raise Exception(f"Pré-requisito B6 ausente: {required_table}")

checks = []


def check(name: str, ok: bool, detail: str = ""):
    prefix = "✅" if ok else "❌"
    msg = f"{prefix} {name}"
    if detail:
        msg += f" — {detail}"
    checks.append((ok, msg))
    print(msg)


baseline = spark.table(BASELINE_SILVER)
replay = spark.table(REPLAY_SILVER)
source = spark.table(SOURCE_SILVER)
latest_bronze = spark.table(BRONZE).agg(F.max(SNAPSHOT)).collect()[0][0]

print("\n=== GATE B6 — CONTROLLED REPLAY IDEMPOTENCY ===\n")

baseline_rows = baseline.count()
replay_rows = replay.count()
source_rows = source.count()
check("Cardinalidade replay inalterada", replay_rows == baseline_rows, f"antes={baseline_rows:,} | depois={replay_rows:,}")

baseline_ids = baseline.select("id").distinct().count()
replay_ids = replay.select("id").distinct().count()
check("IDs distintos inalterados", replay_ids == baseline_ids, f"antes={baseline_ids:,} | depois={replay_ids:,}")

baseline_versioned = baseline.groupBy("id").count().filter(F.col("count") > 1).count()
replay_versioned = replay.groupBy("id").count().filter(F.col("count") > 1).count()
check("IDs versionados inalterados", replay_versioned == baseline_versioned, f"antes={baseline_versioned:,} | depois={replay_versioned:,}")

baseline_current = baseline.filter(F.col("is_current")).count()
replay_current = replay.filter(F.col("is_current")).count()
check("Versões current inalteradas", replay_current == baseline_current, f"antes={baseline_current:,} | depois={replay_current:,}")

# Prova forte: multiconjunto de linhas deve ser exatamente igual antes e depois.
# exceptAll preserva multiplicidade, então detecta tanto linhas alteradas quanto duplicação.
missing_after_replay = baseline.exceptAll(replay).count()
unexpected_after_replay = replay.exceptAll(baseline).count()
check(
    "Conteúdo exato da Silver sandbox é idêntico antes/depois",
    missing_after_replay == 0 and unexpected_after_replay == 0,
    f"faltando={missing_after_replay:,} | inesperadas={unexpected_after_replay:,}",
)

# A Silver real também precisa permanecer exatamente igual à baseline criada antes do replay.
source_missing = baseline.exceptAll(source).count()
source_unexpected = source.exceptAll(baseline).count()
check(
    "Silver real não foi tocada pelo B6",
    source_rows == baseline_rows and source_missing == 0 and source_unexpected == 0,
    f"faltando={source_missing:,} | inesperadas={source_unexpected:,}",
)

replay_rows_control = spark.table(REPLAY_CONTROL).filter(F.col("entity") == ENTITY).collect()
control_ok = False
control_detail = f"linhas={len(replay_rows_control)}"
if len(replay_rows_control) == 1:
    row = replay_rows_control[0]
    control_ok = (
        row["last_processed_snapshot"] == latest_bronze
        and row["candidate_snapshot"] is None
        and row["status"] == "COMMITTED"
    )
    control_detail = (
        f"committed={row['last_processed_snapshot']} | candidate={row['candidate_snapshot']} | status={row['status']}"
    )
check("Watermark sandbox voltou ao último snapshot", control_ok, control_detail)

real_rows = spark.table(REAL_CONTROL).filter(F.col("entity") == ENTITY).collect()
real_control_ok = False
real_detail = f"linhas={len(real_rows)}"
if len(real_rows) == 1:
    row = real_rows[0]
    real_control_ok = (
        row["last_processed_snapshot"] == latest_bronze
        and row["candidate_snapshot"] is None
        and row["status"] == "COMMITTED"
    )
    real_detail = (
        f"committed={row['last_processed_snapshot']} | candidate={row['candidate_snapshot']} | status={row['status']}"
    )
check("Watermark real não foi alterado", real_control_ok, real_detail)

failed = [msg for ok, msg in checks if not ok]
print(f"\n=== RESULTADO B6: {len(checks) - len(failed)}/{len(checks)} checks passaram ===")

if failed:
    print("❌ Sandbox preservada para investigação; nada será limpo automaticamente.")
    raise Exception("Gate B6 falhou:\n" + "\n".join(failed))

# Cleanup só depois da prova completa.
for table_name in [REPLAY_CONTROL, REPLAY_SILVER, BASELINE_SILVER]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

print("✅ Replay controlado aprovado: reprocessar o último snapshot não mudou uma única linha.")
print("✅ Tabelas sandbox B6 removidas após sucesso.")
