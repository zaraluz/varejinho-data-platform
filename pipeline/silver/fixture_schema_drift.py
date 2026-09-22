# Databricks notebook source
# Gate S2 — fixture isolada do engine canônico de Schema Drift.
#
# Prova:
# 1) NO_DRIFT
# 2) ADDITIVE permitido + evento persistido + baseline NÃO promovido
# 3) REMOVED_COLUMN bloqueado
# 4) TYPE_CHANGE bloqueado
# 5) PROMOTION explícita de evento additive
# 6) REPLAY após promoção sem novo drift
#
# Usa apenas control storage sandbox. Não altera tabelas de negócio.

from __future__ import annotations

import importlib.util
import os
import uuid

from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def job_param(nome: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(nome)
        return value if value else default
    except Exception:
        return default


def derive_bundle_files_path() -> str:
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
            f"Notebook S2 fora do layout esperado do bundle: {workspace_path}"
        )
    return workspace_path.split(marker, 1)[0]


CATALOG = job_param("catalog", "varejinho_dev")
BUNDLE_FILES_PATH = job_param("bundle_files_path", derive_bundle_files_path())
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate S2 de Schema Drift é dev-only durante hardening. Recebido: {CATALOG}"
    )

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_engine.py"
if not os.path.isfile(ENGINE_PATH):
    raise Exception(f"Schema drift engine não encontrado: {ENGINE_PATH}")

spec = importlib.util.spec_from_file_location("varejinho_schema_drift_engine", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise Exception(f"Não foi possível carregar o engine: {ENGINE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

SchemaDriftEngine = module.SchemaDriftEngine
SchemaDriftViolation = module.SchemaDriftViolation

RUN_ID = uuid.uuid4().hex[:12]
SANDBOX_ROOT = f"{CONTROL_ROOT}/_schema_drift_fixture/{RUN_ID}"
ENTITY = "schema_drift_fixture"
engine = SchemaDriftEngine(dbutils=dbutils, control_root=SANDBOX_ROOT)

print("\n=== S2 PATH RESOLUTION ===")
print(f"bundle_files_path: {BUNDLE_FILES_PATH}")
print(f"engine_path:       {ENGINE_PATH}")
print(f"sandbox_root:      {SANDBOX_ROOT}")
print("Nenhuma tabela de negócio será alterada.\n")

base_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amount", IntegerType(), True),
    ]
)
base_df = spark.createDataFrame([("A", 10), ("B", 20)], base_schema)

additive_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amount", IntegerType(), True),
        StructField("new_col", StringType(), True),
    ]
)
additive_df = spark.createDataFrame(
    [("A", 10, "x"), ("B", 20, "y")],
    additive_schema,
)

removed_schema = StructType([StructField("id", StringType(), False)])
removed_df = spark.createDataFrame([("A",), ("B",)], removed_schema)

type_change_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amount", StringType(), True),
    ]
)
type_change_df = spark.createDataFrame([("A", "10"), ("B", "20")], type_change_schema)

checks = []


def ok(name: str, detail: str = ""):
    checks.append((name, True, detail))
    print(f"✅ {name}" + (f" — {detail}" if detail else ""))


def fail(name: str, detail: str):
    checks.append((name, False, detail))
    print(f"❌ {name} — {detail}")


try:
    # Baseline é criado apenas por operação explícita da fixture.
    baseline = engine.bootstrap_baseline(
        ENTITY,
        base_df,
        approved_by="fixture",
        reason="controlled S2 bootstrap",
    )
    baseline_hash_v1 = baseline["schema_hash"]

    # 1) NO_DRIFT
    stable_df, stable_report = engine.evaluate(ENTITY, base_df, tier="critical")
    if (
        stable_report["classification"] == "no_drift"
        and stable_report["event_id"] is None
        and stable_df.columns == ["id", "amount"]
    ):
        ok("NO_DRIFT", "sem evento e schema preservado")
    else:
        fail("NO_DRIFT", str(stable_report))

    # 2) ADDITIVE permitido, mas sem promoção automática.
    projected_df, additive_report = engine.evaluate(
        ENTITY,
        additive_df,
        tier="critical",
    )
    schema_after_additive, meta_after_additive = engine.load_baseline(ENTITY)
    additive_event_exists = engine._exists(additive_report["event_path"])
    if (
        additive_report["classification"] == "additive"
        and additive_report["action"] == "ALLOW_WITH_BASELINE_PROJECTION"
        and projected_df.columns == ["id", "amount"]
        and schema_after_additive == {"id": "string", "amount": "int"}
        and meta_after_additive["schema_hash"] == baseline_hash_v1
        and additive_event_exists
    ):
        ok(
            "ADDITIVE allow+log",
            f"event={additive_report['event_id']} | baseline não mudou",
        )
    else:
        fail("ADDITIVE allow+log", str(additive_report))

    # 3) REMOVED_COLUMN bloqueado e baseline permanece intacto.
    try:
        engine.evaluate(ENTITY, removed_df, tier="critical")
        fail("REMOVED_COLUMN block", "evaluate não bloqueou")
    except SchemaDriftViolation:
        schema_after_removed, meta_after_removed = engine.load_baseline(ENTITY)
        removed_event_id = engine._event_id(
            ENTITY,
            schema_after_removed,
            engine.schema_of(removed_df),
        )
        removed_event_path = engine.event_path(ENTITY, removed_event_id)
        if (
            meta_after_removed["schema_hash"] == baseline_hash_v1
            and engine._exists(removed_event_path)
        ):
            ok("REMOVED_COLUMN block", f"event={removed_event_id}")
        else:
            fail("REMOVED_COLUMN block", "baseline mudou ou evento não persistiu")

    # 4) TYPE_CHANGE bloqueado e baseline permanece intacto.
    try:
        engine.evaluate(ENTITY, type_change_df, tier="critical")
        fail("TYPE_CHANGE block", "evaluate não bloqueou")
    except SchemaDriftViolation:
        schema_after_type, meta_after_type = engine.load_baseline(ENTITY)
        type_event_id = engine._event_id(
            ENTITY,
            schema_after_type,
            engine.schema_of(type_change_df),
        )
        type_event_path = engine.event_path(ENTITY, type_event_id)
        if (
            meta_after_type["schema_hash"] == baseline_hash_v1
            and engine._exists(type_event_path)
        ):
            ok("TYPE_CHANGE block", f"event={type_event_id}")
        else:
            fail("TYPE_CHANGE block", "baseline mudou ou evento não persistiu")

    # 5) PROMOTION explícita do evento additive.
    promoted = engine.promote_event(
        ENTITY,
        additive_report["event_id"],
        approved_by="fixture",
        reason="approve additive schema in controlled S2 fixture",
    )
    schema_v2, meta_v2 = engine.load_baseline(ENTITY)
    if (
        schema_v2 == {"id": "string", "amount": "int", "new_col": "string"}
        and int(meta_v2["version"]) == 2
        and meta_v2["schema_hash"] != baseline_hash_v1
        and engine._exists(promoted["promotion_path"])
    ):
        ok("EXPLICIT PROMOTION", f"baseline version={meta_v2['version']}")
    else:
        fail("EXPLICIT PROMOTION", str(promoted))

    # 6) REPLAY: após promoção, o mesmo schema não gera novo drift/evento.
    replay_df, replay_report = engine.evaluate(
        ENTITY,
        additive_df,
        tier="critical",
    )
    if (
        replay_report["classification"] == "no_drift"
        and replay_report["event_id"] is None
        and replay_df.columns == ["id", "amount", "new_col"]
    ):
        ok("REPLAY AFTER PROMOTION", "no_drift")
    else:
        fail("REPLAY AFTER PROMOTION", str(replay_report))

    passed = sum(1 for _, success, _ in checks if success)
    failed = len(checks) - passed

    print("\n=== S2 RESULT ===")
    print(f"{passed}/{len(checks)} checks passaram | {failed} falharam")

    if failed:
        raise Exception(
            "S2 fixture falhou:\n"
            + "\n".join(
                f"- {name}: {detail}"
                for name, success, detail in checks
                if not success
            )
        )

    print("✅ Schema drift engine provado em isolamento.")
    print("✅ Detectar drift não promove baseline automaticamente.")
    print("✅ Breaking drift persiste evidência antes de bloquear.")
    print("✅ Promoção é explícita e deixa trilha auditável.")

    # Cleanup somente após sucesso completo.
    dbutils.fs.rm(SANDBOX_ROOT, recurse=True)
    print("✅ Sandbox de control storage removido após sucesso.")

except Exception:
    print(f"ℹ️ Sandbox preservado para diagnóstico: {SANDBOX_ROOT}")
    raise
