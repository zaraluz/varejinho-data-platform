# Databricks notebook source
# Gate C3 — fixture controlada do engine canônico de Data Contracts.
#
# Prova isoladamente:
# PASS / QUARANTINE / WARNING / FAIL missing column / FAIL type / FAIL missing contract.
# Não altera tabelas de negócio.

from datetime import datetime, timedelta
from decimal import Decimal
import importlib.util
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate C3 de contracts é dev-only durante hardening. Recebido: {CATALOG}"
    )

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_engine.py"
CONTRACT_PATH = f"{BUNDLE_FILES_PATH}/contracts/fixtures/contract_engine_fixture.yaml"

spec = importlib.util.spec_from_file_location("varejinho_contract_engine", ENGINE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ContractValidator = module.ContractValidator
ContractViolation = module.ContractViolation

validator = ContractValidator(
    CONTRACT_PATH,
    spark=spark,
    catalog=CATALOG,
    schema="silver",
)

loja_table = f"{CATALOG}.silver.loja"
if not spark.catalog.tableExists(loja_table):
    raise Exception(f"Fixture requer referência existente: {loja_table}")

loja_row = (
    spark.table(loja_table)
    .where(F.col("id").isNotNull())
    .select(F.col("id").cast("string").alias("id"))
    .limit(1)
    .collect()
)
if not loja_row:
    raise Exception(f"Fixture requer ao menos 1 loja em {loja_table}")

valid_loja = loja_row[0]["id"]
missing_loja = f"__fixture_missing_{uuid.uuid4().hex}__"
if spark.table(loja_table).filter(F.col("id").cast("string") == missing_loja).count():
    raise Exception("Falha improvável: id de loja fixture já existe")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("id_loja", StringType(), True),
    StructField("amount", DecimalType(14, 2), True),
])

REFERENCE_TIME = datetime(2026, 9, 22, 12, 0, 0)
FRESH = REFERENCE_TIME - timedelta(hours=1)
STALE = REFERENCE_TIME - timedelta(days=10)

checks = []


def check(name: str, passed: bool, detail: str = ""):
    checks.append((name, passed, detail))
    status = "✅" if passed else "❌"
    print(f"{status} {name} {detail}")


print("\n=== GATE C3 — CONTRACT ENGINE FIXTURE ===")
print(f"Catalog: {CATALOG}")
print("Nenhuma tabela de negócio será alterada.\n")

# 1) PASS — linha válida, RI válida, freshness válida.
pass_df = spark.createDataFrame(
    [("pass-1", FRESH, valid_loja, Decimal("10.00"))],
    schema=schema,
)
pass_valid, pass_quar, pass_report = validator.validate(
    pass_df,
    reference_time=REFERENCE_TIME,
)
check(
    "PASS",
    pass_valid.count() == 1
    and pass_quar.count() == 0
    and pass_report["warning_count"] == 0,
    str(pass_report),
)

# 2) QUARANTINE — duplicata, range inválido e not-null inválido.
quarantine_df = spark.createDataFrame(
    [
        ("dup-1", FRESH, valid_loja, Decimal("10.00")),
        ("dup-1", FRESH, valid_loja, Decimal("11.00")),
        ("neg-1", FRESH, valid_loja, Decimal("-1.00")),
        ("null-1", FRESH, valid_loja, None),
    ],
    schema=schema,
)
q_valid, q_quar, q_report = validator.validate(
    quarantine_df,
    reference_time=REFERENCE_TIME,
)
reasons = [r["_contract_reason"] for r in q_quar.select("_contract_reason").collect()]
check(
    "QUARANTINE",
    q_valid.count() == 0
    and q_quar.count() == 4
    and any("no_duplicates" in r for r in reasons)
    and any("min:" in r for r in reasons)
    and any("not_null:" in r for r in reasons),
    f"quarantine={q_report['quarantine']} reasons={reasons}",
)

# 3) WARNING — RI e freshness violadas, mas linha continua válida.
warning_df = spark.createDataFrame(
    [("warn-1", STALE, missing_loja, Decimal("10.00"))],
    schema=schema,
)
w_valid, w_quar, w_report = validator.validate(
    warning_df,
    reference_time=REFERENCE_TIME,
)
warning_rules = {w["rule"] for w in w_report["warnings"]}
check(
    "WARNING",
    w_valid.count() == 1
    and w_quar.count() == 0
    and warning_rules == {"referential_integrity", "freshness"},
    str(w_report["warnings"]),
)

# 4) FAIL CLOSED — coluna obrigatória ausente.
missing_column_failed = False
try:
    validator.validate(
        pass_df.drop("amount"),
        reference_time=REFERENCE_TIME,
    )
except ContractViolation as exc:
    missing_column_failed = "ausente" in str(exc).lower()
check("FAIL missing column", missing_column_failed)

# 5) FAIL CLOSED — tipo diferente do contrato.
wrong_type_failed = False
try:
    validator.validate(
        pass_df.withColumn("amount", F.col("amount").cast("string")),
        reference_time=REFERENCE_TIME,
    )
except ContractViolation as exc:
    wrong_type_failed = "type mismatch" in str(exc).lower()
check("FAIL type mismatch", wrong_type_failed)

# 6) FAIL CLOSED — contrato obrigatório inexistente.
missing_contract_failed = False
try:
    ContractValidator(
        f"{BUNDLE_FILES_PATH}/contracts/fixtures/__missing_contract__.yaml",
        spark=spark,
        catalog=CATALOG,
        schema="silver",
    )
except ContractViolation as exc:
    missing_contract_failed = "ausente" in str(exc).lower()
check("FAIL missing contract", missing_contract_failed)

passed = sum(1 for _, ok, _ in checks if ok)
failed = len(checks) - passed
print(f"\n=== C3 RESULT ===\n{passed}/{len(checks)} checks passaram | {failed} falharam")

if failed:
    failures = [f"{name}: {detail}" for name, ok, detail in checks if not ok]
    raise Exception("Gate C3 falhou:\n" + "\n".join(failures))

print("\n✅ Contract engine provado em isolamento. Ainda NÃO integrado aos runtimes Silver.")
