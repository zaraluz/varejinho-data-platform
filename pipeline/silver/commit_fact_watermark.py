# Databricks notebook source
# pipeline/silver/commit_fact_watermark.py
# Promove candidate_snapshot -> last_processed_snapshot após validação.

import importlib.util

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
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


CATALOG = required_param("catalog")
ENTITY = job_param("entity", "all")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")
CONTROL_ROOT = required_param("control_root").rstrip("/")
BRONZE_SOURCE_CATALOG = required_param("bronze_source_catalog")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
BRONZE_OVERRIDE = job_param("bronze_table", "")

FACTS = [
    "notaentrada", "notaentradaitem", "perda", "logestoque",
    "promocao", "promocaoitem", "pedido", "pedidoitem", "oferta",
    "pagarfornecedor", "pagarfornecedorparcela",
    "pagaroutrasdespesas", "pagaroutrasdespesasimposto",
]


PARTITION_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest_runtime.py"
_partition_spec = importlib.util.spec_from_file_location(
    "varejinho_partition_manifest_runtime_commit",
    PARTITION_RUNTIME_PATH,
)
if _partition_spec is None or _partition_spec.loader is None:
    raise ImportError(
        f"Não foi possível carregar partition manifest runtime: {PARTITION_RUNTIME_PATH}"
    )
_partition_module = importlib.util.module_from_spec(_partition_spec)
_partition_spec.loader.exec_module(_partition_module)
FactPartitionManifestRuntime = _partition_module.FactPartitionManifestRuntime
MUTATION_GUARD = FactPartitionManifestRuntime(
    spark=spark,
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
    bundle_files_path=BUNDLE_FILES_PATH,
    bronze_source_catalog=BRONZE_SOURCE_CATALOG,
)


def commit(entity):
    rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )
    if len(rows) != 1:
        raise Exception(f"{entity}: watermark esperado=1; encontrado={len(rows)}")

    row = rows[0]
    committed = row["last_processed_snapshot"]
    candidate = row["candidate_snapshot"]
    status = row["status"]

    if status == "COMMITTED" and candidate is None:
        print(f"ℹ️ {entity}: já COMMITTED; nada a promover.")
        return

    if status != "PENDING_VALIDATION" or candidate is None:
        raise Exception(
            f"{entity}: estado inválido para commit: status={status}, "
            f"candidate={candidate}"
        )

    bronze = BRONZE_OVERRIDE or f"{CATALOG}.bronze.{entity}"
    bronze_max = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]

    if committed is not None and candidate < committed:
        raise Exception(
            f"{entity}: candidate {candidate} anterior ao committed {committed}"
        )
    if candidate > bronze_max:
        raise Exception(
            f"{entity}: candidate {candidate} à frente da Bronze {bronze_max}"
        )

    manifest_promote = MUTATION_GUARD.promote(
        entity,
        committed,
        candidate,
        bronze_override=BRONZE_OVERRIDE,
    )
    print(
        f"[MUTATION_GUARD] {entity}: manifest promotion ready "
        f"| rows={manifest_promote['promoted_rows']} "
        f"| reused={manifest_promote['reused']}"
    )

    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET last_processed_snapshot = candidate_snapshot,
            candidate_snapshot = NULL,
            status = 'COMMITTED',
            updated_at = current_timestamp()
        WHERE entity = '{entity}'
          AND status = 'PENDING_VALIDATION'
    """)

    final = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()[0]
    )
    if final["status"] != "COMMITTED" or final["candidate_snapshot"] is not None:
        raise Exception(f"{entity}: falha ao promover watermark")

    post_commit_manifest = MUTATION_GUARD.assert_committed(
        entity,
        final["last_processed_snapshot"],
        bronze_override=BRONZE_OVERRIDE,
    )
    print(
        f"[MUTATION_GUARD] {entity}: post-commit history verified "
        f"| partitions={post_commit_manifest.get('manifest_rows', 0)}"
    )

    print(
        f"✅ {entity}: watermark {committed} -> "
        f"{final['last_processed_snapshot']} | status=COMMITTED"
    )


entities = FACTS if ENTITY == "all" else [ENTITY]
for entity in entities:
    commit(entity)

print("\n✅ Commit de fact_watermark concluído.")
