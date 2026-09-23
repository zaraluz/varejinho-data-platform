# Databricks notebook source
# pipeline/silver/validate_mature_incremental_facts.py
# Validação batch-level para fatos incrementais com partições maduras.
# Expected e runtime usam os mesmos engines canônicos de Drift + Contracts.
#
# Semântica:
# - compara apenas o lote (committed, candidate]
# - exige que todas as chaves esperadas no lote existam na Silver com valores idênticos
# - permite chaves históricas extras na Silver (política no-delete)
# - falha se a Silver contiver qualquer linha com ingestion_date > candidate

from functools import reduce
import importlib.util

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(nome)
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


CATALOG = required_param("catalog")
ENTITY = job_param("entity", "all")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")
CONTROL_ROOT = required_param("control_root")
BRONZE_SOURCE_CATALOG = required_param("bronze_source_catalog")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
BRONZE_OVERRIDE = job_param("bronze_table", "")
SILVER_OVERRIDE = job_param("silver_table", "")


CONTRACT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_runtime.py"
_runtime_spec = importlib.util.spec_from_file_location(
    "varejinho_contract_runtime_validate_mature", CONTRACT_RUNTIME_PATH
)
if _runtime_spec is None or _runtime_spec.loader is None:
    raise ImportError(f"Não foi possível carregar contract runtime: {CONTRACT_RUNTIME_PATH}")
_contract_runtime_module = importlib.util.module_from_spec(_runtime_spec)
_runtime_spec.loader.exec_module(_contract_runtime_module)
SilverContractRuntime = _contract_runtime_module.SilverContractRuntime
CONTRACTS = SilverContractRuntime(
    spark=spark,
    catalog=CATALOG,
    bundle_files_path=BUNDLE_FILES_PATH,
)

DRIFT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_runtime.py"
_drift_spec = importlib.util.spec_from_file_location(
    "varejinho_schema_drift_runtime_validate_mature", DRIFT_RUNTIME_PATH
)
if _drift_spec is None or _drift_spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema drift runtime: {DRIFT_RUNTIME_PATH}")
_drift_module = importlib.util.module_from_spec(_drift_spec)
_drift_spec.loader.exec_module(_drift_module)
SilverSchemaDriftRuntime = _drift_module.SilverSchemaDriftRuntime
DRIFT = SilverSchemaDriftRuntime(
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
    bundle_files_path=BUNDLE_FILES_PATH,
)

PARTITION_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest_runtime.py"
_partition_spec = importlib.util.spec_from_file_location(
    "varejinho_partition_manifest_runtime_validate_mature",
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

CONFIG = {
    "notaentrada": {"chave": ["numeronota", "id_loja", "id_fornecedor"], "data": "dataentrada", "decimais": ["valortotal", "valormercadoria", "valordesconto"]},
    "notaentradaitem": {"chave": ["id"], "data": None, "decimais": ["quantidade", "valor", "valortotal"]},
    "perda": {"chave": ["id"], "data": "data", "decimais": ["quantidade", "valor"]},
    "logestoque": {"chave": ["id"], "data": "datamovimento", "decimais": ["quantidade", "estoqueanterior", "estoqueatual", "custocomimposto", "custosemimposto", "customediocomimposto", "customediosemimposto"]},
    "promocao": {"chave": ["id"], "data": "datainicio", "decimais": ["valor", "valordesconto"]},
    "promocaoitem": {"chave": ["id"], "data": None, "decimais": ["precovenda"]},
    "pedido": {"chave": ["id"], "data": "datacompra", "decimais": []},
    "pedidoitem": {"chave": ["id"], "data": None, "decimais": ["quantidade", "custocompra", "valortotal"]},
    "oferta": {"chave": ["id"], "data": "datainicio", "decimais": ["precooferta", "preconormal"], "try_decimais": ["precoimediato"]},
    "pagarfornecedor": {"chave": ["id"], "data": "dataemissao", "decimais": ["valor"]},
    "pagarfornecedorparcela": {"chave": ["id"], "data": "datavencimento", "decimais": ["valor", "valoracrescimo"], "datas_extras": ["datapagamento", "datapagamentocontabil"]},
    "pagaroutrasdespesas": {"chave": ["id"], "data": "dataemissao", "decimais": ["valor", "valorbruto"]},
    "pagaroutrasdespesasimposto": {"chave": ["id"], "data": "datavencimento", "decimais": ["valor", "basecalculo", "aliquota"]},
}


def aplicar_casts(df, cfg):
    for col in cfg["decimais"]:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"),
            )

    for col in cfg.get("try_decimais", []):
        if col in df.columns:
            df = df.withColumn(
                col,
                F.expr(f"try_cast(replace(`{col}`, ',', '.') as decimal(14,3))"),
            )

    if cfg["data"] and cfg["data"] in df.columns:
        df = (
            df.withColumn(
                cfg["data"],
                F.to_timestamp(F.col(cfg["data"]), "yyyy/MM/dd HH:mm:ss.SSS"),
            )
            .withColumn("ano", F.year(cfg["data"]))
            .withColumn("mes", F.month(cfg["data"]))
        )

    for col in cfg.get("datas_extras", []):
        if col in df.columns:
            df = df.withColumn(
                col,
                F.expr(
                    f"try_to_timestamp(`{col}`, 'yyyy/MM/dd HH:mm:ss.SSS')"
                ),
            )
    return df


def validar(entity):
    cfg = CONFIG[entity]
    keys = cfg["chave"]
    bronze = BRONZE_OVERRIDE or f"{CATALOG}.bronze.{entity}"
    silver = SILVER_OVERRIDE or f"{CATALOG}.silver.{entity}"
    validator = CONTRACTS.validator(entity, keys)

    rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )
    if len(rows) != 1:
        raise Exception(f"{entity}: watermark esperado=1; encontrado={len(rows)}")

    state = rows[0]
    committed = state["last_processed_snapshot"]
    candidate = state["candidate_snapshot"]
    status = state["status"]

    if status == "COMMITTED" and candidate is None:
        print(f"ℹ️ {entity}: sem candidate pendente; nada a validar.")
        return

    if status != "PENDING_VALIDATION" or candidate is None:
        raise Exception(
            f"{entity}: estado inválido para validação: "
            f"committed={committed}, candidate={candidate}, status={status}"
        )

    batch = spark.table(bronze)
    if committed is not None:
        batch = batch.filter(F.col("ingestion_date") > F.lit(committed))
    batch = batch.filter(F.col("ingestion_date") <= F.lit(candidate))

    typed_batch = aplicar_casts(batch, cfg)
    accepted_batch, drift_report = DRIFT.evaluate(entity, typed_batch)
    expected, _, contract_report = CONTRACTS.validate_snapshot_history(
        validator,
        accepted_batch,
        keys,
    )
    CONTRACTS.log_report(f"validate_mature:{entity}", contract_report)

    actual = spark.table(silver)

    e_schema = {f.name: f.dataType.simpleString() for f in expected.schema.fields}
    a_schema = {f.name: f.dataType.simpleString() for f in actual.schema.fields}
    schema_ok = e_schema == a_schema

    e_dup = expected.groupBy(*keys).count().filter(F.col("count") > 1).count()
    a_dup = actual.groupBy(*keys).count().filter(F.col("count") > 1).count()

    expected_keys = expected.select(*keys)
    actual_keys = actual.select(*keys)

    missing = expected_keys.join(
        actual_keys, on=keys, how="left_anti"
    ).count()

    future_rows = actual.filter(
        F.col("ingestion_date") > F.lit(candidate)
    ).count()

    mismatches = None
    mismatch_df = None

    if schema_ok and e_dup == 0 and a_dup == 0:
        cols = expected.columns
        nonkeys = [c for c in cols if c not in keys]

        e = expected.select(*cols).alias("e")
        a = actual.select(*cols).alias("a")

        join_condition = reduce(
            lambda acc, k: acc & F.col(f"e.{k}").eqNullSafe(F.col(f"a.{k}")),
            keys[1:],
            F.col(f"e.{keys[0]}").eqNullSafe(F.col(f"a.{keys[0]}")),
        )

        joined = e.join(a, join_condition, how="inner")

        if nonkeys:
            diff_condition = reduce(
                lambda acc, c: acc
                | (~F.col(f"e.{c}").eqNullSafe(F.col(f"a.{c}"))),
                nonkeys[1:],
                ~F.col(f"e.{nonkeys[0]}").eqNullSafe(
                    F.col(f"a.{nonkeys[0]}")
                ),
            )
            mismatch_df = joined.filter(diff_condition)
            mismatches = mismatch_df.count()
        else:
            mismatches = 0

    ok = (
        schema_ok
        and e_dup == 0
        and a_dup == 0
        and missing == 0
        and future_rows == 0
        and mismatches == 0
    )

    print(f"\n=== VALIDATE MATURE FACT INCREMENTAL — {entity} ===")
    print(f"committed:          {committed}")
    print(f"candidate:          {candidate}")
    print(f"drift:              {drift_report['classification']}")
    print(f"batch expected rows:{expected.count():,}")
    print(f"silver total rows:  {actual.count():,}")
    print(f"schema exact:       {schema_ok}")
    print(f"duplicate keys:     expected={e_dup:,} | actual={a_dup:,}")
    print(f"missing batch keys: {missing:,}")
    print(f"value mismatches:   {mismatches}")
    print(f"rows > candidate:   {future_rows:,}")
    print(f"RESULT:             {'✅ PASS' if ok else '❌ FAIL'}")

    if not ok:
        if missing:
            print("\nAmostra de chaves do lote ausentes na Silver:")
            (
                expected_keys.join(actual_keys, on=keys, how="left_anti")
                .limit(10)
                .show(truncate=False)
            )

        if mismatch_df is not None and mismatches:
            print("\nAmostra de chaves com payload divergente:")
            mismatch_df.select(
                *[F.col(f"e.{k}").alias(k) for k in keys]
            ).limit(10).show(truncate=False)

        if future_rows:
            print("\nAmostra de linhas na Silver acima do candidate:")
            (
                actual.filter(F.col("ingestion_date") > F.lit(candidate))
                .select(*keys, "ingestion_date")
                .orderBy(F.col("ingestion_date").desc())
                .limit(20)
                .show(truncate=False)
            )

        raise Exception(
            f"{entity}: lote maduro divergiu ou Silver contém dados acima "
            f"do candidate={candidate}"
        )

    manifest_stage = MUTATION_GUARD.stage(
        entity,
        committed,
        candidate,
        bronze_override=BRONZE_OVERRIDE,
    )
    print(
        f"[MUTATION_GUARD] {entity}: validated manifest staged "
        f"| rows={manifest_stage['staged_rows']} "
        f"| reused={manifest_stage['reused']}"
    )


entities = list(CONFIG) if ENTITY == "all" else [ENTITY]
for entity in entities:
    if entity not in CONFIG:
        raise Exception(f"Entidade não suportada: {entity}")
    validar(entity)

print("\n✅ Validação batch-level das partições maduras concluída.")
print("✅ Expected foi produzido pelos mesmos engines de Drift + Contracts do APPLY.")
print("✅ Chaves históricas extras são permitidas pela política no-delete.")
print("✅ Nenhum watermark foi committed por esta task.")