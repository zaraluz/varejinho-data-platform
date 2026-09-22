# Databricks notebook source
# pipeline/silver/profile_schema_drift.py
# Gate S1 — inventário READ-ONLY do estado atual de Schema Drift.
#
# NÃO cria baseline.
# NÃO promove schema.
# NÃO grava drift event.
# NÃO altera Bronze/Silver/Gold/control.

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType


def job_param(nome: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(nome)
        return value if value else default
    except Exception:
        return default


def resolve_bundle_files_path() -> str:
    explicit = job_param("bundle_files_path", "")
    if explicit:
        return explicit.rstrip("/")

    try:
        raw = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        # notebookPath() normalmente retorna /Users/... no Databricks,
        # enquanto arquivos sincronizados pelo bundle são acessíveis via /Workspace/Users/...
        workspace_path = raw if raw.startswith("/Workspace/") else f"/Workspace{raw}"
        marker = "/pipeline/silver/profile_schema_drift"
        if marker in workspace_path:
            return workspace_path.split(marker, 1)[0]
    except Exception:
        pass

    # Fallback apenas para compatibilidade de execução local/manual antiga.
    return "/Workspace/Users/<USER>/varejinho-data-platform"


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")
BUNDLE_FILES_PATH = resolve_bundle_files_path()
REGISTRY_ROOT = f"{CONTROL_ROOT}/schema_registry"
POLICY_PATH = f"{BUNDLE_FILES_PATH}/contracts/silver/_policy.yaml"

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Gate S1 de Schema Drift é dev-only durante hardening. Recebido: {CATALOG}"
    )

if not os.path.isfile(POLICY_PATH):
    raise Exception(f"Policy de contracts não encontrada: {POLICY_PATH}")

with open(POLICY_PATH, "r", encoding="utf-8") as f:
    policy = yaml.safe_load(f) or {}

entity_policy = {}
for tier, cfg in (policy.get("tiers", {}) or {}).items():
    for entity in cfg.get("entities", []) or []:
        entity_policy[entity] = tier

if len(entity_policy) != 37:
    raise Exception(
        f"Esperadas 37 entidades na policy; encontrado={len(entity_policy)}"
    )

ACTIVE_INCREMENTAL_FACTS = {
    "venda",
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
}

SCD2_ENTITIES = {"produto", "fornecedor", "mercadologico"}


def runtime_group(entity: str) -> str:
    if entity in ACTIVE_INCREMENTAL_FACTS:
        return "incremental_fact"
    if entity in SCD2_ENTITIES:
        return "scd2"
    if entity == "curvaabc":
        return "snapshot_dimension"
    return "reference_dimension"


def safe_ls(path: str):
    try:
        return dbutils.fs.ls(path)
    except Exception:
        return []


def basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def iso_mtime(ms):
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return str(ms)


registry_entries = safe_ls(REGISTRY_ROOT)
baseline_files = {
    basename(info.path)[:-5]: info
    for info in registry_entries
    if basename(info.path).endswith(".json")
}

drift_log_entries = safe_ls(f"{REGISTRY_ROOT}/drift_log")
drift_logs_by_entity = {entity: 0 for entity in entity_policy}
for info in drift_log_entries:
    name = basename(info.path)
    for entity in entity_policy:
        if name.startswith(f"{entity}_"):
            drift_logs_by_entity[entity] += 1
            break

rows = []
for entity in sorted(entity_policy):
    tier = entity_policy[entity]
    group = runtime_group(entity)
    baseline_info = baseline_files.get(entity)
    baseline_exists = baseline_info is not None
    baseline_valid = False
    baseline_schema = {}
    baseline_error = None

    if baseline_exists:
        try:
            raw = dbutils.fs.head(baseline_info.path)
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("baseline JSON não é objeto coluna->tipo")
            baseline_schema = {str(k): str(v) for k, v in parsed.items()}
            baseline_valid = True
        except Exception as exc:
            baseline_error = str(exc)[:300]

    silver_table = f"{CATALOG}.silver.{entity}"
    silver_exists = spark.catalog.tableExists(silver_table)
    silver_schema = {}
    if silver_exists:
        silver_schema = {
            field.name: field.dataType.simpleString()
            for field in spark.table(silver_table).schema.fields
        }

    added_vs_baseline = []
    removed_vs_baseline = []
    changed_vs_baseline = {}
    exact_vs_silver = None

    if baseline_valid and silver_exists:
        added_vs_baseline = sorted(set(silver_schema) - set(baseline_schema))
        removed_vs_baseline = sorted(set(baseline_schema) - set(silver_schema))
        changed_vs_baseline = {
            col: {
                "baseline": baseline_schema[col],
                "silver": silver_schema[col],
            }
            for col in sorted(set(baseline_schema) & set(silver_schema))
            if baseline_schema[col] != silver_schema[col]
        }
        exact_vs_silver = not (
            added_vs_baseline or removed_vs_baseline or changed_vs_baseline
        )

    current_owner = (
        "inline incremental_sales/incremental_facts"
        if entity in ACTIVE_INCREMENTAL_FACTS
        else "none in active runtime"
    )

    rows.append(
        {
            "entity": entity,
            "tier": tier,
            "runtime_group": group,
            "current_drift_owner": current_owner,
            "baseline_exists": baseline_exists,
            "baseline_json_valid": baseline_valid,
            "baseline_columns": len(baseline_schema),
            "baseline_modified_utc": iso_mtime(
                getattr(baseline_info, "modificationTime", None)
                if baseline_info is not None
                else None
            ),
            "silver_exists": silver_exists,
            "silver_columns": len(silver_schema),
            "baseline_exact_vs_silver": exact_vs_silver,
            "added_vs_baseline": json.dumps(added_vs_baseline, ensure_ascii=False),
            "removed_vs_baseline": json.dumps(removed_vs_baseline, ensure_ascii=False),
            "type_changes_vs_baseline": json.dumps(
                changed_vs_baseline, ensure_ascii=False
            ),
            "persisted_drift_logs": drift_logs_by_entity.get(entity, 0),
            "baseline_error": baseline_error,
        }
    )

PROFILE_SCHEMA = StructType(
    [
        StructField("entity", StringType(), False),
        StructField("tier", StringType(), False),
        StructField("runtime_group", StringType(), False),
        StructField("current_drift_owner", StringType(), False),
        StructField("baseline_exists", BooleanType(), False),
        StructField("baseline_json_valid", BooleanType(), False),
        StructField("baseline_columns", IntegerType(), False),
        StructField("baseline_modified_utc", StringType(), True),
        StructField("silver_exists", BooleanType(), False),
        StructField("silver_columns", IntegerType(), False),
        StructField("baseline_exact_vs_silver", BooleanType(), True),
        StructField("added_vs_baseline", StringType(), False),
        StructField("removed_vs_baseline", StringType(), False),
        StructField("type_changes_vs_baseline", StringType(), False),
        StructField("persisted_drift_logs", IntegerType(), False),
        StructField("baseline_error", StringType(), True),
    ]
)

profile_df = spark.createDataFrame(rows, schema=PROFILE_SCHEMA)

print("\n=== GATE S1 — SCHEMA DRIFT INVENTORY ===")
print(f"Catalog:           {CATALOG}")
print(f"Control root:      {CONTROL_ROOT}")
print(f"Registry root:     {REGISTRY_ROOT}")
print(f"Bundle files path: {BUNDLE_FILES_PATH}")
print("READ-ONLY: nenhum baseline/evento/dado será alterado.\n")

display(
    profile_df.orderBy(
        F.expr("CASE tier WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END"),
        "entity",
    )
)

summary = profile_df.agg(
    F.count("*").alias("entities"),
    F.sum(F.col("baseline_exists").cast("int")).alias("baselines_found"),
    F.sum(
        (
            F.col("baseline_exists")
            & (F.col("runtime_group") == F.lit("incremental_fact"))
        ).cast("int")
    ).alias("fact_baselines"),
    F.sum(
        (
            F.col("baseline_exists")
            & (F.col("runtime_group") != F.lit("incremental_fact"))
        ).cast("int")
    ).alias("nonfact_baselines"),
    F.sum((~F.col("baseline_json_valid") & F.col("baseline_exists")).cast("int")).alias(
        "invalid_baseline_json"
    ),
    F.sum((F.col("baseline_exact_vs_silver") == F.lit(True)).cast("int")).alias(
        "baseline_exact_vs_silver"
    ),
    F.sum((F.col("baseline_exact_vs_silver") == F.lit(False)).cast("int")).alias(
        "baseline_diff_vs_silver"
    ),
    F.sum("persisted_drift_logs").alias("persisted_drift_logs"),
).collect()[0]

print("=== S1 SUMMARY ===")
for key, value in summary.asDict().items():
    print(f"{key}: {value}")

print("\n=== S1 COVERAGE BY RUNTIME GROUP ===")
(
    profile_df.groupBy("runtime_group")
    .agg(
        F.count("*").alias("entities"),
        F.sum(F.col("baseline_exists").cast("int")).alias("baselines"),
        F.sum("persisted_drift_logs").alias("drift_logs"),
    )
    .orderBy("runtime_group")
    .show(100, truncate=False)
)

print("\n=== S1 ACTIONABLE DETAILS ===")
for row in profile_df.orderBy("entity").collect():
    findings = []
    if not row["baseline_exists"]:
        findings.append("NO_BASELINE")
    if row["baseline_exists"] and not row["baseline_json_valid"]:
        findings.append("INVALID_BASELINE_JSON")
    if row["baseline_exact_vs_silver"] is False:
        findings.append("BASELINE_DIFFERS_FROM_COMMITTED_SILVER")
    if row["current_drift_owner"] == "none in active runtime":
        findings.append("NO_ACTIVE_DRIFT_ENFORCEMENT")
    if row["persisted_drift_logs"] == 0:
        findings.append("NO_PERSISTED_DRIFT_EVENTS")

    if findings:
        print(f"\n[{row['tier'].upper()}] {row['entity']} ({row['runtime_group']})")
        print(f"  findings={findings}")
        print(
            f"  baseline={row['baseline_exists']} valid={row['baseline_json_valid']} "
            f"modified={row['baseline_modified_utc']}"
        )
        if row["baseline_exact_vs_silver"] is False:
            print(f"  added_vs_baseline={row['added_vs_baseline']}")
            print(f"  removed_vs_baseline={row['removed_vs_baseline']}")
            print(f"  type_changes={row['type_changes_vs_baseline']}")
        if row["baseline_error"]:
            print(f"  baseline_error={row['baseline_error']}")

print("\n=== STATIC CODE FINDINGS ALREADY CONFIRMED ===")
print("- pipeline/silver/schema_drift.py exists but is not registered in the active bundle DAG.")
print("- standalone schema_drift.py hardcodes s3://varejinho-lake/_control/schema_registry.")
print("- incremental_sales.py has its own detectar_drift implementation.")
print("- incremental_facts.py has another detectar_drift implementation.")
print("- active incremental drift logic overwrites the baseline after comparison, even when drift exists.")
print("- current active drift enforcement covers venda + 13 facts only; SCD2/reference paths do not call it.")
print("- inline active implementations do not persist a durable drift event; they only print the finding.")
print("\n✅ S1 concluído em modo read-only. Use SUMMARY + ACTIONABLE DETAILS before redesigning drift.")
