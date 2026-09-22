# Databricks notebook source
# Gate C3 — fixture controlada do engine canônico de Data Contracts.
#
# Prova isoladamente:
# PASS / QUARANTINE / WARNING / FAIL missing column / FAIL type /
# FAIL missing contract / snapshot-scoped uniqueness.
# Não altera tabelas de negócio.

from datetime import datetime, timedelta
from decimal import Decimal
import importlib.util
import os
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
        value = dbutils.widgets.get(nome)
        return value if value else default
    except Exception:
        return default


def derive_bundle_files_path() -> str:
    """Resolve o root .../dev/files quando o notebook é executado manualmente."""
    try:
        raw = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise Exception(
            "Não foi possível descobrir o path deste notebook. "
            "Execute via bundle/job ou informe bundle_files_path."
        ) from exc

    workspace_path = raw if raw.startswith("/Workspace/") else f"/Workspace{raw}"
    marker = "/pipeline/silver/"
    if marker not in workspace_path:
        raise Exception(
            f"Notebook C3 fora do layout esperado do bundle: {workspace_path}"
        )
    return workspace_path.split(marker, 1)[0]


CATALOG = job_param("catalog", "varejinho_dev")
BUNDLE_FILES_PATH = job_param("bundle_files_path", derive_bundle_files_path())

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate C3 de contracts é dev-only durante hardening. Recebido: {CATALOG}"
    )

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_engine.py"
CONTRACT_PATH = f"{BUNDLE_FILES_PATH}/contracts/fixtures/contract_engine_fixture.yaml"

print("\n=== C3 PATH RESOLUTION ===")
print(f"bundle_files_path: {BUNDLE_FILES_PATH}")
print(f"engine_path: {ENGINE_PATH}")
print(f"contract_path: {CONTRACT_PATH}")

if not os.path.isfile(ENGINE_PATH):
    raise Exception(f"Engine não encontrado no bundle: {ENGINE_PATH}")
if not os.path.isfile(CONTRACT_PATH):
    raise Exception(f"Contrato fixture não encontrado no bundle: {CONTRACT_PATH}")

spec = importlib.util.spec_from_file_location("varejinho_contract_engine", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise Exception(f"Não foi possível carregar o engine: {ENGINE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

if not hasattr(module, "ContractValidator") or not hasattr(module, "ContractViolation"):
    raise Exception(
        "Engine carregado não é a versão canônica do C3. "
        f"Path resolvido: {ENGINE_PATH}"
    )

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

# 7) UNIQUE POR SNAPSHOT — repetir id em dias distintos é legítimo; no mesmo dia não.
def scoped_row(id_value: str, snapshot: str):
    return (
        pass_df.withColumn("id", F.lit(id_value))
        .withColumn("ingestion_date", F.lit(snapshot).cast("date"))
    )

scope_df = (
    scoped_row("scope-ok", "2026-09-01")
    .unionByName(scoped_row("scope-ok", "2026-09-02"))
    .unionByName(scoped_row("scope-dup", "2026-09-02"))
    .unionByName(scoped_row("scope-dup", "2026-09-02"))
)
s_valid, s_quar, s_report = validator.validate(
    scope_df,
    reference_time=REFERENCE_TIME,
    uniqueness_scope=["ingestion_date"],
)
s_reasons = [
    r["_contract_reason"]
    for r in s_quar.select("_contract_reason").collect()
]
check(
    "SNAPSHOT uniqueness scope",
    s_valid.count() == 2
    and s_quar.count() == 2
    and all("no_duplicates" in r for r in s_reasons)
    and any("scope=ingestion_date" in str(rule.get("detail")) for rule in s_report["rules"]),
    f"valid={s_valid.count()} quarantine={s_quar.count()} reasons={s_reasons}",
)

passed = sum(1 for _, ok, _ in checks if ok)
failed = len(checks) - passed
print(f"\n=== C3 RESULT ===\n{passed}/{len(checks)} checks passaram | {failed} falharam")

if failed:
    failures = [f"{name}: {detail}" for name, ok, detail in checks if not ok]
    raise Exception("Gate C3 falhou:\n" + "\n".join(failures))

print("\n✅ Contract engine + snapshot uniqueness provados. C4 ainda precisa passar nas fixtures de runtime.")