# Databricks notebook source
# validation/facts/validate_incremental_facts.py
# Valida Silver após APPLY incremental e antes do commit do fact_watermark.
# Expected e runtime usam os mesmos engines canônicos de Drift + Contracts.

from functools import reduce
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


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "all")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
BRONZE_OVERRIDE = job_param("bronze_table", "")
SILVER_OVERRIDE = job_param("silver_table", "")

# A fixture D4 isola tabelas e registry. Em runtime real, CONTROL_ROOT permanece intacto.
_is_d4_fixture = "_d4_" in " ".join([BRONZE_OVERRIDE, SILVER_OVERRIDE, CONTROL_TABLE])
DRIFT_CONTROL_ROOT = (
    CONTROL_ROOT
    if (not _is_d4_fixture or CONTROL_ROOT.endswith("/d4"))
    else f"{CONTROL_ROOT}/d4"
)

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"validate_incremental_facts só pode executar em *_dev. Recebido: {CATALOG}"
    )

CONTRACT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_runtime.py"
_runtime_spec = importlib.util.spec_from_file_location(
    "varejinho_contract_runtime_validate", CONTRACT_RUNTIME_PATH
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
    "varejinho_schema_drift_runtime_validate", DRIFT_RUNTIME_PATH
)
if _drift_spec is None or _drift_spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema drift runtime: {DRIFT_RUNTIME_PATH}")
_drift_module = importlib.util.module_from_spec(_drift_spec)
_drift_spec.loader.exec_module(_drift_module)
SilverSchemaDriftRuntime = _drift_module.SilverSchemaDriftRuntime
DRIFT = SilverSchemaDriftRuntime(
    dbutils=dbutils,
    control_root=DRIFT_CONTROL_ROOT,
    bundle_files_path=BUNDLE_FILES_PATH,
)

PARTITION_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest_runtime.py"
_partition_spec = importlib.util.spec_from_file_location(
    "varejinho_partition_manifest_runtime_validate",
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
    control_root=DRIFT_CONTROL_ROOT,
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

    row = rows[0]
    committed = row["last_processed_snapshot"]
    candidate = row["candidate_snapshot"]
    status = row["status"]

    if status == "COMMITTED" and candidate is None:
        print(f"ℹ️ {entity}: sem candidate pendente; nada a validar.")
        return

    if status != "PENDING_VALIDATION" or candidate is None:
        raise Exception(
            f"{entity}: estado inválido para validação: status={status}, "
            f"candidate={candidate}"
        )

    expected_history = (
        spark.table(bronze)
        .filter(F.col("ingestion_date") <= F.lit(candidate))
    )
    expected_history = aplicar_casts(expected_history, cfg)
    accepted_history, drift_report = DRIFT.evaluate(entity, expected_history)
    expected, _, contract_report = CONTRACTS.validate_snapshot_history(
        validator,
        accepted_history,
        keys,
    )
    CONTRACTS.log_report(f"validate:{entity}", contract_report)

    actual = spark.table(silver)

    e_schema = {f.name: f.dataType.simpleString() for f in expected.schema.fields}
    a_schema = {f.name: f.dataType.simpleString() for f in actual.schema.fields}
    schema_ok = e_schema == a_schema

    e_rows = expected.count()
    a_rows = actual.count()
    e_dup = expected.groupBy(*keys).count().filter(F.col("count") > 1).count()
    a_dup = actual.groupBy(*keys).count().filter(F.col("count") > 1).count()

    e_keys = expected.select(*keys)
    a_keys = actual.select(*keys)
    missing = e_keys.join(a_keys, on=keys, how="left_anti").count()
    extra = a_keys.join(e_keys, on=keys, how="left_anti").count()

    mismatches = None
    mismatch_df = None
    nonkeys = []

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
        and e_rows == a_rows
        and e_dup == 0
        and a_dup == 0
        and missing == 0
        and extra == 0
        and mismatches == 0
    )

    print(f"\n=== VALIDATE FACT INCREMENTAL — {entity} ===")
    print(f"candidate:      {candidate}")
    print(f"drift:          {drift_report['classification']}")
    print(f"schema exact:   {schema_ok}")
    print(f"rows:           expected={e_rows:,} | actual={a_rows:,}")
    print(f"duplicate keys: expected={e_dup:,} | actual={a_dup:,}")
    print(f"key coverage:   missing={missing:,} | extra={extra:,}")
    print(f"value mismatch: {mismatches}")
    print(f"RESULT:         {'✅ PASS' if ok else '❌ FAIL'}")

    if not ok:
        print("\n--- DIAGNÓSTICO DA DIVERGÊNCIA ---")

        missing_cols = sorted(set(e_schema) - set(a_schema))
        extra_cols = sorted(set(a_schema) - set(e_schema))
        type_mismatches = sorted(
            [
                f"{col}: expected={e_schema[col]} actual={a_schema[col]}"
                for col in set(e_schema) & set(a_schema)
                if e_schema[col] != a_schema[col]
            ]
        )
        if missing_cols:
            print(f"Schema — colunas ausentes na Silver: {missing_cols}")
        if extra_cols:
            print(f"Schema — colunas extras na Silver: {extra_cols}")
        if type_mismatches:
            print(f"Schema — tipos divergentes: {type_mismatches}")

        if missing > 0:
            print("\nAmostra de chaves esperadas e ausentes na Silver:")
            e_keys.join(a_keys, on=keys, how="left_anti").limit(10).show(truncate=False)

        if extra > 0:
            print("\nAmostra de chaves extras na Silver:")
            a_keys.join(e_keys, on=keys, how="left_anti").limit(10).show(truncate=False)

        if mismatch_df is not None and mismatches > 0:
            print("\nMismatch por coluna:")
            mismatch_exprs = [
                F.sum(
                    F.when(
                        ~F.col(f"e.{col}").eqNullSafe(F.col(f"a.{col}")),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).alias(col)
                for col in nonkeys
            ]
            counts = mismatch_df.agg(*mismatch_exprs).collect()[0].asDict()
            changed_cols = [
                (col, count)
                for col, count in counts.items()
                if count and count > 0
            ]
            changed_cols.sort(key=lambda x: x[1], reverse=True)
            for col, count in changed_cols:
                print(f"  {col}: {count:,}")

        print(
            "\n⚠️ Watermark NÃO será committed. "
            "O estado PENDING_VALIDATION foi preservado para diagnóstico/retry."
        )
        raise Exception(
            f"{entity}: incremental divergiu do full rebuild até {candidate}"
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

print("\n✅ Validação incremental concluída. Nenhum watermark foi committed.")