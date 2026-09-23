# Databricks notebook source
# validation/scd2/prepare_scd2_reappearance_fixture.py
# R3 synthetic fixture — proves SCD2 semantics when an ID disappears and reappears.

from pyspark.sql import functions as F


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param(
    "control_root",
    "s3://varejinho-lake/_control/dev/r3_scd2_reappearance",
).rstrip("/")

FULL_BRONZE = f"{CATALOG}.control._r3_supplier_reappearance_full_bronze"
BASELINE_BRONZE = f"{CATALOG}.control._r3_supplier_reappearance_baseline_bronze"
SILVER = f"{CATALOG}.silver._r3_supplier_reappearance_incremental"
EXPECTED = f"{CATALOG}.silver._r3_supplier_reappearance_expected"
CONTROL = f"{CATALOG}.control._r3_supplier_reappearance_watermark"

if not CATALOG.endswith("_dev"):
    raise Exception(f"R3 fixture is dev-only. Received: {CATALOG}")

# Sandbox reset only.
dbutils.fs.rm(CONTROL_ROOT, True)
for table in [CONTROL, EXPECTED, SILVER, BASELINE_BRONZE, FULL_BRONZE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

schema = """
id string,
razaosocial string,
nomefantasia string,
cnpj string,
id_situacaocadastro string,
telefone string,
permitenfsempedido string,
id_tipoempresa string,
id_tipocustocompra string,
id_tipocustodevolucaotroca string,
pedidominimoqtd string,
pedidominimovalor string,
valormaximoverbapedido string,
id_contacontabilfinanceiro string,
id_fornecedorfavorecido string,
id_municipio string,
datacadastro string,
ingestion_date string
"""

rows = [
    # Anchor supplier keeps every global snapshot visible.
    ("799999","ANCHOR SUPPLIER LTDA","ANCHOR","99999999000199","1","85000000000","N","3","1","1","10","100","500","700","799999","2304400","2026/01/01 00:00:00.000000000","2026-09-01"),
    ("799999","ANCHOR SUPPLIER LTDA","ANCHOR","99999999000199","1","85000000000","N","3","1","1","10","100","500","700","799999","2304400","2026/01/01 00:00:00.000000000","2026-09-02"),
    ("799999","ANCHOR SUPPLIER LTDA","ANCHOR","99999999000199","1","85000000000","N","3","1","1","10","100","500","700","799999","2304400","2026/01/01 00:00:00.000000000","2026-09-03"),
    ("799999","ANCHOR SUPPLIER LTDA","ANCHOR","99999999000199","1","85000000000","N","3","1","1","10","100","500","700","799999","2304400","2026/01/01 00:00:00.000000000","2026-09-04"),
    ("799999","ANCHOR SUPPLIER LTDA","ANCHOR","99999999000199","1","85000000000","N","3","1","1","10","100","500","700","799999","2304400","2026/01/01 00:00:00.000000000","2026-09-05"),

    # Test supplier:
    # D1 present -> D2 absent -> D3 same Type 2 + Type 1 changed.
    ("790001","GAP SUPPLIER LTDA","GAP ONE","55555555000155","1","85111110000","N","3","1","1","10","100","500","701","790001","2304400","2026/01/01 08:00:00.000000000","2026-09-01"),
    ("790001","GAP SUPPLIER LTDA","GAP ONE UPDATED","55555555000155","0","85222220000","Y","8","1","1","10","100","500","701","790001","2304400","2026/01/01 08:00:00.000000000","2026-09-03"),

    # D4 absent -> D5 reappears with a Type 2 change (razaosocial).
    ("790001","GAP SUPPLIER COMERCIO LTDA","GAP ONE UPDATED","55555555000155","0","85222220000","Y","8","1","1","10","100","500","701","790001","2304400","2026/01/01 08:00:00.000000000","2026-09-05"),
]

full = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("ingestion_date", F.to_date("ingestion_date"))
)
baseline = full.filter(F.col("ingestion_date") == F.lit("2026-09-01").cast("date"))

full.write.format("delta").mode("overwrite").saveAsTable(FULL_BRONZE)
baseline.write.format("delta").mode("overwrite").saveAsTable(BASELINE_BRONZE)

spark.sql(
    f"""
    CREATE TABLE {CONTROL} (
        entity STRING,
        last_processed_snapshot DATE,
        candidate_snapshot DATE,
        status STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    """
)
spark.sql(
    f"""
    INSERT INTO {CONTROL}
    VALUES ('fornecedor', DATE '2026-09-01', NULL, 'COMMITTED', current_timestamp())
    """
)

print("\n=== R3 — PREPARE SCD2 REAPPEARANCE FIXTURE ===")
print(f"Baseline Bronze: {BASELINE_BRONZE}")
print(f"Full Bronze:     {FULL_BRONZE}")
print(f"Incremental:     {SILVER}")
print(f"Expected:        {EXPECTED}")
print(f"Control:         {CONTROL}")
print(f"Control root:    {CONTROL_ROOT}")
print("Scenario 1: D1 present -> D2 absent -> D3 same Type 2 + changed Type 1")
print("Scenario 2: D3 present -> D4 absent -> D5 changed Type 2")
print("Expected semantics: absence is a non-event; compare reappearance with last observation.")
print("✅ Synthetic fixture prepared; no real Silver/control state was modified.")
