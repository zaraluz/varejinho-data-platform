# Databricks notebook source
# validation/scd2/bootstrap_scd2_reappearance_drift.py
# R3 fixture control bootstrap — isolated Schema Drift baseline.

import importlib.util


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


def required_param(nome: str) -> str:
    """Parâmetro obrigatório do job: falha cedo em vez de cair num default de ambiente."""
    try:
        value = dbutils.widgets.get(nome)
    except Exception:
        value = ""
    if not value:
        raise ValueError(
            f"Parâmetro obrigatório ausente: '{nome}'. Execute via job do bundle, "
            "que injeta catalog/bundle_files_path/control_root/bronze_source_catalog por target."
        )
    return value


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param(
    "control_root",
    "s3://varejinho-lake/_control/dev/r3_scd2_reappearance",
).rstrip("/")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")
SILVER = f"{CATALOG}.silver._r3_supplier_reappearance_incremental"

if not CATALOG.endswith("_dev"):
    raise Exception(f"R3 fixture is dev-only. Received: {CATALOG}")
if not spark.catalog.tableExists(SILVER):
    raise Exception(f"Fixture Silver baseline missing: {SILVER}")

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_engine.py"
spec = importlib.util.spec_from_file_location("r3_schema_drift_engine", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load Schema Drift engine: {ENGINE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

result = module.SchemaDriftEngine(
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
).bootstrap_baseline(
    entity="fornecedor",
    df=spark.table(SILVER),
    approved_by="fixture:r3",
    reason="R3 synthetic SCD2 reappearance baseline",
)

print("\n=== R3 — SCHEMA DRIFT SANDBOX BASELINE ===")
print(f"Control root: {CONTROL_ROOT}")
print(f"Silver:       {SILVER}")
print(f"Result:       {result}")
print("✅ Isolated drift baseline ready.")
