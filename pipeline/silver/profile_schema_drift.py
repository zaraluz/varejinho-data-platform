# Databricks notebook source
# pipeline/silver/profile_schema_drift.py
# Final read-only audit of the canonical Schema Drift control plane.
#
# DOES NOT create/promote baselines.
# DOES NOT write drift events.
# DOES NOT alter Bronze/Silver/Gold/control data.

from __future__ import annotations

import importlib.util
import json
import os
from datetime import datetime, timezone

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
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
        workspace_path = raw if raw.startswith("/Workspace/") else f"/Workspace{raw}"
        marker = "/pipeline/silver/profile_schema_drift"
        if marker in workspace_path:
            return workspace_path.split(marker, 1)[0]
    except Exception:
        pass

    return "/Workspace/Users/<USER>/varejinho-data-platform"


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")
BUNDLE_FILES_PATH = resolve_bundle_files_path()
REGISTRY_ROOT = f"{CONTROL_ROOT}/schema_registry"
POLICY_PATH = f"{BUNDLE_FILES_PATH}/contracts/silver/_policy.yaml"
ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_engine.py"

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Schema Drift profile is dev-only during hardening. Received: {CATALOG}"
    )

if not os.path.isfile(POLICY_PATH):
    raise Exception(f"Contract policy not found: {POLICY_PATH}")
if not os.path.isfile(ENGINE_PATH):
    raise Exception(f"Schema Drift engine not found: {ENGINE_PATH}")

with open(POLICY_PATH, "r", encoding="utf-8") as f:
    policy = yaml.safe_load(f) or {}

entity_policy = {}
for tier, cfg in (policy.get("tiers", {}) or {}).items():
    for entity in cfg.get("entities", []) or []:
        if entity in entity_policy:
            raise Exception(f"Duplicate entity in policy: {entity}")
        entity_policy[entity] = tier

if len(entity_policy) != 37:
    raise Exception(f"Expected 37 entities in policy; found={len(entity_policy)}")

_engine_spec = importlib.util.spec_from_file_location(
    "varejinho_schema_drift_engine_profile", ENGINE_PATH
)
if _engine_spec is None or _engine_spec.loader is None:
    raise ImportError(f"Could not load Schema Drift engine: {ENGINE_PATH}")
_engine_module = importlib.util.module_from_spec(_engine_spec)
_engine_spec.loader.exec_module(_engine_module)

SchemaDriftEngine = _engine_module.SchemaDriftEngine
ENGINE = SchemaDriftEngine(dbutils=dbutils, control_root=CONTROL_ROOT)

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


def enforcement_path(entity: str) -> str:
    group = runtime_group(entity)
    if group == "incremental_fact":
        return "incremental_sales.py / incremental_facts.py"
    if group == "scd2":
        return "incremental_scd2.py"
    return "transform_reference_dimensions.py"


def safe_ls(path: str):
    try:
        return dbutils.fs.ls(path)
    except Exception:
        return []


def count_json(path: str) -> int:
    return sum(1 for item in safe_ls(path) if item.path.rstrip("/").endswith(".json"))


def iso_mtime(ms):
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return str(ms)


def baseline_mtime(entity: str):
    path = ENGINE.baseline_path(entity)
    parent = path.rsplit("/", 1)[0]
    target = path.rsplit("/", 1)[-1]
    for info in safe_ls(parent):
        if info.path.rstrip("/").endswith("/" + target):
            return iso_mtime(getattr(info, "modificationTime", None))
    return None


rows = []
for entity in sorted(entity_policy):
    tier = entity_policy[entity]
    group = runtime_group(entity)

    baseline_exists = False
    baseline_valid = False
    baseline_schema = {}
    baseline_columns = []
    baseline_version = None
    baseline_format = None
    baseline_error = None

    try:
        baseline_schema, baseline_meta = ENGINE.load_baseline(entity)
        baseline_exists = True
        baseline_valid = True
        baseline_columns = list(baseline_meta.get("columns", []))
        baseline_version = int(baseline_meta.get("version", 1))
        baseline_format = str(baseline_meta.get("format", "envelope"))
    except Exception as exc:
        message = str(exc)
        baseline_exists = "baseline ausente" not in message.lower()
        baseline_error = message[:500]

    silver_table = f"{CATALOG}.silver.{entity}"
    silver_exists = spark.catalog.tableExists(silver_table)
    silver_schema = {}
    silver_columns = []
    if silver_exists:
        silver_df = spark.table(silver_table)
        silver_schema = {
            field.name: field.dataType.simpleString()
            for field in silver_df.schema.fields
        }
        silver_columns = list(silver_df.columns)

    added_vs_baseline = []
    removed_vs_baseline = []
    changed_vs_baseline = {}
    schema_exact = None
    order_exact = None

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
        schema_exact = not (
            added_vs_baseline or removed_vs_baseline or changed_vs_baseline
        )
        order_exact = baseline_columns == silver_columns

    drift_events = count_json(f"{REGISTRY_ROOT}/events/{entity}")
    promotions = count_json(f"{REGISTRY_ROOT}/promotions/{entity}")

    rows.append(
        {
            "entity": entity,
            "tier": tier,
            "runtime_group": group,
            "enforcement_path": enforcement_path(entity),
            "baseline_exists": baseline_exists,
            "baseline_json_valid": baseline_valid,
            "baseline_format": baseline_format,
            "baseline_version": baseline_version,
            "baseline_columns": len(baseline_schema),
            "baseline_modified_utc": baseline_mtime(entity) if baseline_exists else None,
            "silver_exists": silver_exists,
            "silver_columns": len(silver_schema),
            "baseline_exact_vs_silver": schema_exact,
            "column_order_exact_vs_silver": order_exact,
            "added_vs_baseline": json.dumps(added_vs_baseline, ensure_ascii=False),
            "removed_vs_baseline": json.dumps(removed_vs_baseline, ensure_ascii=False),
            "type_changes_vs_baseline": json.dumps(
                changed_vs_baseline, ensure_ascii=False
            ),
            "persisted_drift_events": drift_events,
            "promotions": promotions,
            "baseline_error": baseline_error,
        }
    )

PROFILE_SCHEMA = StructType(
    [
        StructField("entity", StringType(), False),
        StructField("tier", StringType(), False),
        StructField("runtime_group", StringType(), False),
        StructField("enforcement_path", StringType(), False),
        StructField("baseline_exists", BooleanType(), False),
        StructField("baseline_json_valid", BooleanType(), False),
        StructField("baseline_format", StringType(), True),
        StructField("baseline_version", IntegerType(), True),
        StructField("baseline_columns", IntegerType(), False),
        StructField("baseline_modified_utc", StringType(), True),
        StructField("silver_exists", BooleanType(), False),
        StructField("silver_columns", IntegerType(), False),
        StructField("baseline_exact_vs_silver", BooleanType(), True),
        StructField("column_order_exact_vs_silver", BooleanType(), True),
        StructField("added_vs_baseline", StringType(), False),
        StructField("removed_vs_baseline", StringType(), False),
        StructField("type_changes_vs_baseline", StringType(), False),
        StructField("persisted_drift_events", IntegerType(), False),
        StructField("promotions", IntegerType(), False),
        StructField("baseline_error", StringType(), True),
    ]
)

profile_df = spark.createDataFrame(rows, schema=PROFILE_SCHEMA)

print("\n=== SCHEMA DRIFT FINAL READ-ONLY AUDIT ===")
print(f"Catalog:           {CATALOG}")
print(f"Control root:      {CONTROL_ROOT}")
print(f"Registry root:     {REGISTRY_ROOT}")
print(f"Bundle files path: {BUNDLE_FILES_PATH}")
print("READ-ONLY: no baseline/event/data will be changed.\n")

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
    F.sum(
        (~F.col("baseline_json_valid") & F.col("baseline_exists")).cast("int")
    ).alias("invalid_baseline_json"),
    F.sum(
        (F.col("baseline_exact_vs_silver") == F.lit(True)).cast("int")
    ).alias("baseline_exact_vs_silver"),
    F.sum(
        (F.col("baseline_exact_vs_silver") == F.lit(False)).cast("int")
    ).alias("baseline_diff_vs_silver"),
    F.sum(
        (F.col("column_order_exact_vs_silver") == F.lit(True)).cast("int")
    ).alias("column_order_exact_vs_silver"),
    F.sum(
        (F.col("column_order_exact_vs_silver") == F.lit(False)).cast("int")
    ).alias("column_order_diff_vs_silver"),
    F.sum("persisted_drift_events").alias("persisted_drift_events"),
    F.sum("promotions").alias("promotions"),
).collect()[0]

print("=== FINAL SUMMARY ===")
for key, value in summary.asDict().items():
    print(f"{key}: {value}")

print("\n=== COVERAGE BY RUNTIME GROUP ===")
(
    profile_df.groupBy("runtime_group")
    .agg(
        F.count("*").alias("entities"),
        F.sum(F.col("baseline_exists").cast("int")).alias("baselines"),
        F.sum(
            (F.col("baseline_exact_vs_silver") == F.lit(True)).cast("int")
        ).alias("exact_vs_silver"),
        F.sum("persisted_drift_events").alias("drift_events"),
    )
    .orderBy("runtime_group")
    .show(100, truncate=False)
)

print("\n=== ACTIONABLE DETAILS ===")
actionable = 0
for row in profile_df.orderBy("entity").collect():
    findings = []
    if not row["baseline_exists"]:
        findings.append("NO_BASELINE")
    elif not row["baseline_json_valid"]:
        findings.append("INVALID_BASELINE_JSON")
    if not row["silver_exists"]:
        findings.append("SILVER_TABLE_MISSING")
    if row["baseline_exact_vs_silver"] is False:
        findings.append("BASELINE_DIFFERS_FROM_COMMITTED_SILVER")
    if row["column_order_exact_vs_silver"] is False:
        findings.append("COLUMN_ORDER_DIFFERS_FROM_COMMITTED_SILVER")

    if findings:
        actionable += 1
        print(f"\n[{row['tier'].upper()}] {row['entity']} ({row['runtime_group']})")
        print(f"  findings={findings}")
        print(
            f"  baseline={row['baseline_exists']} valid={row['baseline_json_valid']} "
            f"version={row['baseline_version']} format={row['baseline_format']}"
        )
        print(f"  enforcement={row['enforcement_path']}")
        if row["baseline_exact_vs_silver"] is False:
            print(f"  added_vs_baseline={row['added_vs_baseline']}")
            print(f"  removed_vs_baseline={row['removed_vs_baseline']}")
            print(f"  type_changes={row['type_changes_vs_baseline']}")
        if row["baseline_error"]:
            print(f"  baseline_error={row['baseline_error']}")

if actionable == 0:
    print("✅ No actionable registry/schema findings.")

print("\n=== CANONICAL ARCHITECTURE ===")
print("- quality/schema_drift_engine.py owns compare/classify/event/promotion semantics.")
print("- quality/schema_drift_runtime.py is the active Silver adapter for all 37 entities.")
print("- Runtime cannot bootstrap a missing baseline.")
print("- additive -> persist event + continue with accepted baseline projection.")
print("- removed/type/mixed breaking -> persist event + BLOCK.")
print("- baseline promotion is explicit and auditable.")
print("- active enforcement covers 14 facts + 19 reference dimensions + curvaabc + 3 SCD2.")
print("- pipeline/silver/schema_drift.py is legacy/non-DAG cleanup, not an active owner.")

print("\n✅ Final Schema Drift profile completed in read-only mode.")
