# Databricks notebook source
# Gate S3A — bootstrap EXPLÍCITO dos baselines de Schema Drift ainda ausentes.
#
# Escopo:
# - preserva os 14 baselines já existentes de venda + 13 facts;
# - cria somente baselines ausentes para SCD2/reference/snapshot a partir da
#   Silver committed já validada;
# - nunca sobrescreve baseline existente;
# - valida schema + ordem física de TODOS os 37 baselines ao final.
#
# Este notebook altera apenas CONTROL STORAGE em --target dev.
# Não altera Bronze/Silver/Gold.

from __future__ import annotations

import importlib.util
import os

import yaml


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
            f"Notebook S3A fora do layout esperado do bundle: {workspace_path}"
        )
    return workspace_path.split(marker, 1)[0]


CATALOG = job_param("catalog", "varejinho_dev")
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")
BUNDLE_FILES_PATH = job_param("bundle_files_path", derive_bundle_files_path())

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Bootstrap de Schema Drift é dev-only durante hardening. Recebido: {CATALOG}"
    )

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_engine.py"
POLICY_PATH = f"{BUNDLE_FILES_PATH}/contracts/silver/_policy.yaml"

if not os.path.isfile(ENGINE_PATH):
    raise Exception(f"Schema drift engine não encontrado: {ENGINE_PATH}")
if not os.path.isfile(POLICY_PATH):
    raise Exception(f"Policy não encontrada: {POLICY_PATH}")

spec = importlib.util.spec_from_file_location("varejinho_schema_drift_engine", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise Exception(f"Não foi possível carregar engine: {ENGINE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SchemaDriftEngine = module.SchemaDriftEngine
SchemaDriftViolation = module.SchemaDriftViolation

with open(POLICY_PATH, "r", encoding="utf-8") as f:
    policy = yaml.safe_load(f) or {}

entity_policy = {}
for tier, cfg in (policy.get("tiers", {}) or {}).items():
    for entity in cfg.get("entities", []) or []:
        entity_policy[entity] = tier

if len(entity_policy) != 37:
    raise Exception(f"Policy deveria conter 37 entidades; encontrado={len(entity_policy)}")

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

NONFACT_ENTITIES = sorted(set(entity_policy) - ACTIVE_INCREMENTAL_FACTS)
if len(NONFACT_ENTITIES) != 23:
    raise Exception(f"Esperadas 23 entidades non-fact; encontrado={len(NONFACT_ENTITIES)}")

engine = SchemaDriftEngine(dbutils=dbutils, control_root=CONTROL_ROOT)


def exists(path: str) -> bool:
    try:
        dbutils.fs.head(path)
        return True
    except Exception:
        return False


def silver_schema(entity: str):
    table = f"{CATALOG}.silver.{entity}"
    if not spark.catalog.tableExists(table):
        raise Exception(f"Silver obrigatória ausente para bootstrap: {table}")
    df = spark.table(table)
    return (
        {field.name: field.dataType.simpleString() for field in df.schema.fields},
        list(df.columns),
        df.limit(0),
    )


def verify_baseline(entity: str):
    baseline_schema, meta = engine.load_baseline(entity)
    actual_schema, actual_columns, _ = silver_schema(entity)
    baseline_columns = list(meta.get("columns") or baseline_schema.keys())

    schema_ok = baseline_schema == actual_schema
    order_ok = baseline_columns == actual_columns
    if not schema_ok or not order_ok:
        raise Exception(
            f"{entity}: baseline não reproduz Silver committed | "
            f"schema_ok={schema_ok} order_ok={order_ok} | "
            f"baseline_columns={baseline_columns} | silver_columns={actual_columns}"
        )
    return meta


print("\n=== GATE S3A — EXPLICIT SCHEMA BASELINE BOOTSTRAP ===")
print(f"Catalog:           {CATALOG}")
print(f"Control root:      {CONTROL_ROOT}")
print(f"Bundle files path: {BUNDLE_FILES_PATH}")
print("Fonte de aceitação: schema da Silver committed já validada.")
print("Nenhuma tabela Bronze/Silver/Gold será alterada.\n")

# PRE-FLIGHT: os 14 facts precisam continuar com baseline existente e válido.
missing_fact_baselines = [
    entity
    for entity in sorted(ACTIVE_INCREMENTAL_FACTS)
    if not exists(engine.baseline_path(entity))
]
if missing_fact_baselines:
    raise Exception(
        "S3A não cria/repara baselines de facts. Ausentes: "
        + ", ".join(missing_fact_baselines)
    )

for entity in sorted(ACTIVE_INCREMENTAL_FACTS):
    verify_baseline(entity)
print("✅ PRE-FLIGHT: 14/14 baselines de facts preservados e alinhados à Silver.")

created = []
already_present = []

for entity in NONFACT_ENTITIES:
    path = engine.baseline_path(entity)
    if exists(path):
        verify_baseline(entity)
        already_present.append(entity)
        print(f"↪️ {entity}: baseline já existe e está alinhado; sem overwrite.")
        continue

    _, _, schema_df = silver_schema(entity)
    payload = engine.bootstrap_baseline(
        entity,
        schema_df,
        approved_by="manual-s3a-bootstrap",
        reason=(
            "Explicit bootstrap from committed Silver after Data Contracts E2E "
            "(Silver QG 85/85; Gold QG 51/51)"
        ),
    )
    created.append(entity)
    print(
        f"✅ {entity}: baseline v{payload['version']} criado "
        f"({len(payload['columns'])} colunas)"
    )

# POST-FLIGHT: 37/37 precisam existir e reproduzir schema + ordem da Silver.
verified = []
for entity in sorted(entity_policy):
    verify_baseline(entity)
    verified.append(entity)

print("\n=== S3A RESULT ===")
print(f"entities_policy:       {len(entity_policy)}")
print(f"fact_baselines_kept:   {len(ACTIVE_INCREMENTAL_FACTS)}")
print(f"nonfact_created:       {len(created)}")
print(f"nonfact_already_there: {len(already_present)}")
print(f"verified_exact:        {len(verified)}")
print("expected_total:        37")

if len(verified) != 37:
    raise Exception(f"Post-flight incompleto: verified={len(verified)}")

print("\n✅ S3A bootstrap concluído: 37/37 baselines reproduzem a Silver committed.")
print("✅ Nenhum baseline existente foi sobrescrito.")
print("✅ Próximo passo: integrar o engine aos runtimes; runtime continua proibido de bootstrap automático.")
