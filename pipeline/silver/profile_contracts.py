# Databricks notebook source
# pipeline/silver/profile_contracts.py
# Gate C1 — inventário/lint read-only dos Data Contracts Silver.
#
# NÃO altera Bronze/Silver/Gold.
# Objetivo: medir cobertura, consistência YAML, hardcodes e aderência ao schema real
# antes de ligar enforcement central.

import json
import os
import yaml

from pyspark.sql import functions as F


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
        f"Gate C1 de contracts é dev-only durante hardening. Recebido: {CATALOG}"
    )

CONTRACT_DIR = f"{BUNDLE_FILES_PATH}/contracts/silver"
POLICY_PATH = f"{CONTRACT_DIR}/_policy.yaml"

if not os.path.isfile(POLICY_PATH):
    raise Exception(f"Contract policy ausente: {POLICY_PATH}")

with open(POLICY_PATH, "r", encoding="utf-8") as f:
    policy = yaml.safe_load(f) or {}

tiers_cfg = policy.get("tiers", {})
if not tiers_cfg:
    raise Exception("Contract policy sem tiers")

entity_policy = {}
for tier, cfg in tiers_cfg.items():
    for entity in cfg.get("entities", []):
        if entity in entity_policy:
            raise Exception(
                f"Entidade classificada em mais de um tier: {entity} "
                f"({entity_policy[entity]['tier']} e {tier})"
            )
        entity_policy[entity] = {
            "tier": tier,
            "required": bool(cfg.get("contract_required", False)),
            "enforcement": cfg.get("enforcement", "unknown"),
        }

if len(entity_policy) != 37:
    raise Exception(
        f"Policy deve classificar exatamente as 37 entidades Bronze/Silver. "
        f"Encontrado={len(entity_policy)}"
    )

ROOT_ALLOWED = {
    "table", "owner", "description", "sla_freshness_hours",
    "grain", "columns", "quality_rules",
}
COLUMN_ALLOWED = {
    "name", "type", "nullable", "unique", "min", "max",
    "accepted_values", "description",
}
RULE_ALLOWED = {
    "rule", "key", "columns", "column", "references", "severity",
    "max_age_hours", "min", "max", "accepted_values",
}


def compact_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def tier_order(tier):
    return {"critical": 1, "high": 2, "standard": 3}.get(tier, 9)


details = []

for entity in sorted(entity_policy):
    meta = entity_policy[entity]
    contract_path = f"{CONTRACT_DIR}/{entity}.yaml"
    silver_table = f"{CATALOG}.silver.{entity}"
    exists = os.path.isfile(contract_path)

    row = {
        "entity": entity,
        "tier": meta["tier"],
        "contract_required": meta["required"],
        "enforcement_policy": meta["enforcement"],
        "contract_exists": exists,
        "silver_table_exists": spark.catalog.tableExists(silver_table),
        "yaml_parse_ok": False,
        "malformed_root_keys": "[]",
        "malformed_column_keys": "[]",
        "malformed_rule_keys": "[]",
        "hardcoded_table": False,
        "hardcoded_references": 0,
        "missing_declared_columns": "[]",
        "type_mismatches": "[]",
        "grain_missing_columns": "[]",
        "declared_rule_types": "[]",
        "declared_error_rules": 0,
        "declared_warning_rules": 0,
        "notes": "",
    }

    if not exists:
        row["notes"] = (
            "MISSING_REQUIRED_CONTRACT"
            if meta["required"]
            else "STANDARD_WITHOUT_FULL_CONTRACT"
        )
        details.append(row)
        continue

    try:
        with open(contract_path, "r", encoding="utf-8") as f:
            contract = yaml.safe_load(f) or {}
        row["yaml_parse_ok"] = True
    except Exception as e:
        row["notes"] = f"YAML_PARSE_ERROR:{str(e)[:240]}"
        details.append(row)
        continue

    root_extra = sorted(set(contract) - ROOT_ALLOWED)
    row["malformed_root_keys"] = compact_json(root_extra)

    table_declared = str(contract.get("table", "") or "")
    row["hardcoded_table"] = table_declared.startswith("varejinho.")

    columns_cfg = contract.get("columns", []) or []
    malformed_columns = []
    duplicate_names = []
    names_seen = set()
    declared_types = {}

    for idx, col_cfg in enumerate(columns_cfg):
        if not isinstance(col_cfg, dict):
            malformed_columns.append(f"index={idx}:not_mapping")
            continue

        extra = sorted(set(col_cfg) - COLUMN_ALLOWED)
        if extra:
            malformed_columns.append(
                f"{col_cfg.get('name', f'index={idx}')}:{extra}"
            )

        name = col_cfg.get("name")
        if not name:
            malformed_columns.append(f"index={idx}:missing_name")
            continue

        if name in names_seen:
            duplicate_names.append(name)
        names_seen.add(name)
        declared_types[name] = (
            str(col_cfg.get("type", "") or "").lower().replace(" ", "")
        )

    row["malformed_column_keys"] = compact_json(
        malformed_columns + [f"duplicate:{x}" for x in duplicate_names]
    )

    rules = contract.get("quality_rules", []) or []
    malformed_rules = []
    rule_types = []
    warning_rules = 0
    error_rules = 0
    hardcoded_refs = 0

    for idx, rule_cfg in enumerate(rules):
        if not isinstance(rule_cfg, dict):
            malformed_rules.append(f"index={idx}:not_mapping")
            continue

        extra = sorted(set(rule_cfg) - RULE_ALLOWED)
        if extra:
            malformed_rules.append(
                f"{rule_cfg.get('rule', f'index={idx}')}:{extra}"
            )

        rule_name = str(rule_cfg.get("rule", "UNKNOWN"))
        rule_types.append(rule_name)

        severity = str(rule_cfg.get("severity", "")).lower()
        if severity == "warning":
            warning_rules += 1
        elif severity == "error":
            error_rules += 1

        ref = str(rule_cfg.get("references", "") or "")
        if ref.startswith("varejinho."):
            hardcoded_refs += 1

    row["malformed_rule_keys"] = compact_json(malformed_rules)
    row["declared_rule_types"] = compact_json(sorted(set(rule_types)))
    row["declared_error_rules"] = error_rules
    row["declared_warning_rules"] = warning_rules
    row["hardcoded_references"] = hardcoded_refs

    grain = contract.get("grain", []) or []
    grain_missing = [c for c in grain if c not in names_seen]
    row["grain_missing_columns"] = compact_json(grain_missing)

    if row["silver_table_exists"]:
        actual_schema = {
            field.name: field.dataType.simpleString().lower().replace(" ", "")
            for field in spark.table(silver_table).schema.fields
        }

        missing_declared = sorted(set(names_seen) - set(actual_schema))
        row["missing_declared_columns"] = compact_json(missing_declared)

        type_mismatches = []
        for name, declared_type in declared_types.items():
            if name not in actual_schema or not declared_type:
                continue
            actual_type = actual_schema[name]
            if declared_type != actual_type:
                type_mismatches.append(
                    f"{name}:contract={declared_type}|actual={actual_type}"
                )
        row["type_mismatches"] = compact_json(type_mismatches)
    else:
        row["notes"] = "SILVER_TABLE_MISSING"

    if not row["notes"]:
        findings = []
        if malformed_columns or malformed_rules or root_extra:
            findings.append("MALFORMED_CONTRACT_STRUCTURE")
        if row["hardcoded_table"] or hardcoded_refs:
            findings.append("ENV_HARDCODE")
        if row["missing_declared_columns"] != "[]":
            findings.append("STALE_COLUMNS")
        if row["type_mismatches"] != "[]":
            findings.append("TYPE_MISMATCH")
        if rules:
            findings.append("QUALITY_RULES_DECLARED_NOT_CENTRALLY_ENFORCED")
        row["notes"] = "|".join(findings) if findings else "STATICALLY_ALIGNED"

    details.append(row)


detail_df = spark.createDataFrame(details)

print("\n=== GATE C1 — DATA CONTRACT INVENTORY / LINT ===")
print(f"Catalog: {CATALOG}")
print(f"Entidades classificadas: {len(entity_policy)}")
print("Este gate é READ-ONLY: nenhum dado foi alterado.\n")

display(
    detail_df.orderBy(
        F.expr("CASE tier WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END"),
        "entity",
    )
)

summary = (
    detail_df.agg(
        F.count("*").alias("entities"),
        F.sum(F.col("contract_exists").cast("int")).alias("contracts_found"),
        F.sum(
            (F.col("contract_required") & ~F.col("contract_exists")).cast("int")
        ).alias("required_missing"),
        F.sum(F.col("hardcoded_table").cast("int")).alias("hardcoded_tables"),
        F.sum("hardcoded_references").alias("hardcoded_references"),
        F.sum(
            (F.col("malformed_column_keys") != F.lit("[]")).cast("int")
        ).alias("malformed_contracts"),
        F.sum(
            (F.col("missing_declared_columns") != F.lit("[]")).cast("int")
        ).alias("contracts_with_stale_columns"),
        F.sum(
            (F.col("type_mismatches") != F.lit("[]")).cast("int")
        ).alias("contracts_with_type_mismatch"),
    )
    .collect()[0]
)

print("\n=== C1 SUMMARY ===")
for key in summary.asDict():
    print(f"{key}: {summary[key]}")

print("\n=== C1 DETAIL — ACTIONABLE FINDINGS ===")
for row in sorted(details, key=lambda r: (tier_order(r["tier"]), r["entity"])):
    actionable = (
        not row["contract_exists"]
        or row["malformed_root_keys"] != "[]"
        or row["malformed_column_keys"] != "[]"
        or row["malformed_rule_keys"] != "[]"
        or row["hardcoded_table"]
        or row["hardcoded_references"] > 0
        or row["missing_declared_columns"] != "[]"
        or row["type_mismatches"] != "[]"
        or row["grain_missing_columns"] != "[]"
    )
    if not actionable:
        continue

    print(f"\n[{row['tier'].upper()}] {row['entity']}")
    print(
        f"  contract_exists={row['contract_exists']} | "
        f"required={row['contract_required']} | "
        f"silver_table_exists={row['silver_table_exists']}"
    )
    print(
        f"  hardcoded_table={row['hardcoded_table']} | "
        f"hardcoded_references={row['hardcoded_references']}"
    )
    if row["malformed_root_keys"] != "[]":
        print(f"  malformed_root_keys={row['malformed_root_keys']}")
    if row["malformed_column_keys"] != "[]":
        print(f"  malformed_column_keys={row['malformed_column_keys']}")
    if row["malformed_rule_keys"] != "[]":
        print(f"  malformed_rule_keys={row['malformed_rule_keys']}")
    if row["grain_missing_columns"] != "[]":
        print(f"  grain_missing_columns={row['grain_missing_columns']}")
    if row["missing_declared_columns"] != "[]":
        print(f"  stale_columns={row['missing_declared_columns']}")
    if row["type_mismatches"] != "[]":
        print(f"  type_mismatches={row['type_mismatches']}")
    print(f"  notes={row['notes']}")

print("\n=== DECLARED RULE TYPES ===")
(
    detail_df.select(
        "entity", "tier", "declared_rule_types",
        "declared_error_rules", "declared_warning_rules"
    )
    .where(F.col("contract_exists"))
    .orderBy("entity")
    .show(100, truncate=False)
)

print("\n=== CURRENT ENFORCEMENT GAP ===")
print("- quality/contract_engine.py existe, mas só aplica nullable/min.")
print("- incremental_sales.py e incremental_facts.py duplicam essa lógica inline.")
print("- quality_rules (no_duplicates/RI/freshness/severity) não são interpretadas por um engine central.")
print("- contrato ausente em incremental_facts.py hoje é fail-open.")
print("- tipos declarados não são validados pelo runtime atual.")
print("\n✅ C1 concluído em modo read-only. Use SUMMARY + DETAIL para canonicalizar os contratos antes de enforcement.")
