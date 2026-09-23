# Databricks notebook source
# ops/seed/bootstrap_fact_partition_manifests.py
# R2 explicit one-time bootstrap for already-committed fact partitions.
#
# Creates manifests from the current physical Bronze file state through each
# entity's committed watermark. Existing manifests are never overwritten.

import importlib.util

from pyspark.sql import functions as F


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
CONTROL_ROOT = job_param(
    "control_root",
    "s3://varejinho-lake/_control/dev",
).rstrip("/")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)
CONTROL_TABLE = f"{CATALOG}.control.fact_watermark"

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Manifest bootstrap is dev-only during hardening. Received: {CATALOG}"
    )

ENGINE_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest.py"
spec = importlib.util.spec_from_file_location("partition_manifest_bootstrap", ENGINE_PATH)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load partition manifest engine: {ENGINE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
FactPartitionManifestGuard = module.FactPartitionManifestGuard

GUARD = FactPartitionManifestGuard(
    spark=spark,
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
)

ENTITIES = [
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
]

if not spark.catalog.tableExists(CONTROL_TABLE):
    raise Exception(f"Watermark table missing: {CONTROL_TABLE}")

print("\n=== R2 — FACT PARTITION MANIFEST BOOTSTRAP ===")
print(f"Catalog:      {CATALOG}")
print(f"Bronze:       {BRONZE_SOURCE_CATALOG}.bronze.*")
print(f"Control root: {CONTROL_ROOT}")
print("Explicit bootstrap only; existing manifests are verified, never overwritten.\n")

created = 0
reused = 0
verified = 0

for entity in ENTITIES:
    rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )
    if len(rows) != 1:
        raise Exception(
            f"{entity}: expected exactly one watermark row; found={len(rows)}"
        )

    row = rows[0]
    committed = row["last_processed_snapshot"]
    candidate = row["candidate_snapshot"]
    status = row["status"]

    if status != "COMMITTED" or candidate is not None or committed is None:
        raise Exception(
            f"{entity}: bootstrap requires stable COMMITTED watermark; "
            f"committed={committed} candidate={candidate} status={status}"
        )

    source = f"{BRONZE_SOURCE_CATALOG}.bronze.{entity}"
    result = GUARD.bootstrap(entity, source, committed)

    if result["created"]:
        created += 1
    else:
        reused += 1
    if result["verification"]["ok"]:
        verified += 1

    print(
        f"✅ {entity}: committed={committed} | "
        f"manifest_rows={result['rows']} | "
        f"{'CREATED' if result['created'] else 'VERIFIED_EXISTING'}"
    )

print("\n=== R2 BOOTSTRAP RESULT ===")
print(f"entities: {len(ENTITIES)}")
print(f"created:  {created}")
print(f"reused:   {reused}")
print(f"verified: {verified}")

if verified != len(ENTITIES):
    raise Exception(
        f"Manifest bootstrap verification incomplete: {verified}/{len(ENTITIES)}"
    )

print("✅ 14/14 fact partition manifests reproduce current committed Bronze history.")
