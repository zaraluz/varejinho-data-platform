# Databricks notebook source
# pipeline/silver/prepare_scd2_product_generic_regression.py
# Gate B7H — prepara regressão do produto contra o engine SCD2 genérico.
# Escolhe um snapshot histórico com mudanças reais, cria baseline até o snapshot anterior
# e preserva cópias do estado real para provar que o teste é isolado.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
SOURCE_BRONZE = f"{CATALOG}.bronze.produto"
REAL_SILVER = f"{CATALOG}.silver.produto"
REAL_CONTROL = f"{CATALOG}.control.scd2_watermark"

TEST_BRONZE = f"{CATALOG}.control._b7h_product_test_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._b7h_product_baseline_bronze"
GENERIC_SILVER = f"{CATALOG}.silver._b7h_product_generic"
EXPECTED_SILVER = f"{CATALOG}.silver._b7h_product_expected"
GENERIC_CONTROL = f"{CATALOG}.control._b7h_product_watermark"
REAL_SILVER_BASELINE = f"{CATALOG}.silver._b7h_product_real_baseline"
REAL_CONTROL_BASELINE = f"{CATALOG}.control._b7h_product_real_control_baseline"

KEY = "id"
SNAPSHOT = "ingestion_date"

TYPE2_COLS = [
    "descricaocompleta",
    "mercadologico1",
    "mercadologico2",
    "mercadologico3",
    "ncm1",
    "id_tipoembalagem",
]
TYPE1_COLS = [
    "descricaoreduzida",
    "id_tipomercadoria",
    "pesoliquido",
    "pesobruto",
]

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B7H só pode executar em *_dev. Recebido: {CATALOG}")

for table in [SOURCE_BRONZE, REAL_SILVER, REAL_CONTROL]:
    if not spark.catalog.tableExists(table):
        raise Exception(f"Pré-requisito ausente: {table}")

# Limpa resíduos anteriores.
for table in [
    GENERIC_CONTROL, GENERIC_SILVER, EXPECTED_SILVER,
    BASELINE_BRONZE, TEST_BRONZE,
    REAL_SILVER_BASELINE, REAL_CONTROL_BASELINE,
]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

bronze = spark.table(SOURCE_BRONZE)
snapshots = [
    r[SNAPSHOT]
    for r in bronze.select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()
]
if len(snapshots) < 2:
    raise Exception("Gate B7H requer pelo menos 2 snapshots Bronze de produto")

def hash_expr(alias: str):
    return F.md5(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(f"{alias}.{c}").cast("string"), F.lit("<NULL>"))
                for c in TYPE2_COLS
            ],
        )
    )

# Escolhe automaticamente um snapshot que realmente exercite o engine.
# Prioridade: mais mudanças Type 2; depois novos ids; depois mudanças Type 1.
candidates = []
for idx in range(1, len(snapshots)):
    prev_date = snapshots[idx - 1]
    curr_date = snapshots[idx]

    prev = bronze.filter(F.col(SNAPSHOT) == F.lit(prev_date)).alias("p")
    curr = bronze.filter(F.col(SNAPSHOT) == F.lit(curr_date)).alias("c")

    joined = curr.join(prev, F.col("c.id") == F.col("p.id"), "left")

    new_ids = joined.filter(F.col("p.id").isNull()).count()

    c_hash = hash_expr("c")
    p_hash = hash_expr("p")
    type2_changes = joined.filter(
        F.col("p.id").isNotNull() & (c_hash != p_hash)
    ).count()

    t1_change_expr = None
    for col_name in TYPE1_COLS:
        expr = ~F.col(f"c.{col_name}").eqNullSafe(F.col(f"p.{col_name}"))
        t1_change_expr = expr if t1_change_expr is None else (t1_change_expr | expr)

    type1_changes = joined.filter(
        F.col("p.id").isNotNull() & t1_change_expr
    ).count()

    candidates.append({
        "snapshot": curr_date,
        "previous": prev_date,
        "new_ids": new_ids,
        "type2": type2_changes,
        "type1": type1_changes,
    })

selected = max(
    candidates,
    key=lambda x: (
        1 if x["type2"] > 0 else 0,
        x["type2"],
        x["new_ids"],
        x["type1"],
    ),
)

replay_snapshot = selected["snapshot"]
previous_snapshot = selected["previous"]

# Bronze de teste = história até o snapshot escolhido.
# Bronze baseline = história até o snapshot imediatamente anterior.
spark.sql(f"""
    CREATE TABLE {TEST_BRONZE}
    USING DELTA
    AS
    SELECT *
    FROM {SOURCE_BRONZE}
    WHERE {SNAPSHOT} <= DATE '{replay_snapshot}'
""")
spark.sql(f"""
    CREATE TABLE {BASELINE_BRONZE}
    USING DELTA
    AS
    SELECT *
    FROM {SOURCE_BRONZE}
    WHERE {SNAPSHOT} <= DATE '{previous_snapshot}'
""")

# Controle sandbox começa no snapshot anterior.
spark.sql(f"""
    CREATE TABLE {GENERIC_CONTROL} (
        entity STRING,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING,
        updated_at TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {GENERIC_CONTROL}
    VALUES ('produto', DATE '{previous_snapshot}', NULL, 'COMMITTED', current_timestamp())
""")

# Fotografias do estado real: o teste não pode alterá-las.
spark.sql(f"""
    CREATE TABLE {REAL_SILVER_BASELINE}
    USING DELTA
    AS SELECT * FROM {REAL_SILVER}
""")
spark.sql(f"""
    CREATE TABLE {REAL_CONTROL_BASELINE}
    USING DELTA
    AS
    SELECT *
    FROM {REAL_CONTROL}
""")

print("\n=== GATE B7H — PREPARE PRODUCT GENERIC REGRESSION ===")
print(f"Bronze real:             {SOURCE_BRONZE}")
print(f"Bronze baseline:         {BASELINE_BRONZE}")
print(f"Bronze de teste:         {TEST_BRONZE}")
print(f"Silver genérica:         {GENERIC_SILVER}")
print(f"Silver expected/backfill:{EXPECTED_SILVER}")
print(f"Control sandbox:         {GENERIC_CONTROL}\n")
print(f"snapshot escolhido:      {replay_snapshot}")
print(f"snapshot anterior:       {previous_snapshot}")
print(f"novos ids:               {selected['new_ids']:,}")
print(f"mudanças Type 2:         {selected['type2']:,}")
print(f"mudanças Type 1:         {selected['type1']:,}")
print("✅ Regressão preparada; estado real preservado em baseline isolada.")
