# Databricks notebook source
# pipeline/silver/incremental_facts.py
# Runtime incremental genérico Bronze -> Silver para fatos.
# APPLY somente: o watermark fica PENDING_VALIDATION até task de commit separada.
# Data Contracts: engine canônico em quality/contract_engine.py via contract_runtime.py.

from datetime import date
import importlib.util
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "all")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
MATURE_CUTOFF_OVERRIDE = job_param("mature_cutoff_override", "")
BRONZE_OVERRIDE = job_param("bronze_table", "")
SILVER_OVERRIDE = job_param("silver_table", "")
QUARANTINE_OVERRIDE = job_param("quarantine_table", "")
HISTORY_OVERRIDE = job_param("quarantine_history_table", "")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"incremental_facts só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
    )

CONTRACT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/contract_runtime.py"
_runtime_spec = importlib.util.spec_from_file_location(
    "varejinho_contract_runtime", CONTRACT_RUNTIME_PATH
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


def detectar_drift(tabela, df):
    schema_atual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    registry_file = f"{CONTROL_ROOT}/schema_registry/{tabela}.json"
    try:
        anterior = json.loads(dbutils.fs.head(registry_file))
    except Exception:
        dbutils.fs.put(registry_file, json.dumps(schema_atual), overwrite=True)
        print(f"[{tabela}] Schema baseline criado em {registry_file}")
        return

    novas = sorted(set(schema_atual) - set(anterior))
    removidas = sorted(set(anterior) - set(schema_atual))
    alteradas = {
        c: {"antes": anterior[c], "depois": schema_atual[c]}
        for c in set(schema_atual) & set(anterior)
        if anterior[c] != schema_atual[c]
    }
    if novas or removidas or alteradas:
        print(f"[DRIFT] {tabela}: novas={novas} removidas={removidas} alteradas={alteradas}")
    else:
        print(f"[{tabela}] Schema sem alterações.")
    dbutils.fs.put(registry_file, json.dumps(schema_atual), overwrite=True)


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


def paths_for(entity):
    return (
        BRONZE_OVERRIDE or f"{CATALOG}.bronze.{entity}",
        SILVER_OVERRIDE or f"{CATALOG}.silver.{entity}",
        QUARANTINE_OVERRIDE or f"{CATALOG}.silver._quarantine_{entity}",
        HISTORY_OVERRIDE or f"{CATALOG}.silver._quarantine_history_{entity}",
    )


def latest_mature_partition(entity):
    if MATURE_CUTOFF_OVERRIDE:
        cutoff = date.fromisoformat(MATURE_CUTOFF_OVERRIDE)
        print(f"[{entity}] mature_cutoff_override={cutoff}")
        return cutoff

    if BRONZE_OVERRIDE:
        raise Exception(
            f"{entity}: bronze_table override exige mature_cutoff_override no sandbox"
        )

    source = f"{BRONZE_SOURCE_CATALOG}.bronze.{entity}"
    maturity = spark.sql(f"""
        SELECT
            ingestion_date,
            MIN(to_date(_metadata.file_modification_time)) AS min_modified_date
        FROM {source}
        GROUP BY ingestion_date
    """)

    return (
        maturity
        .filter(F.col("min_modified_date") > F.col("ingestion_date"))
        .agg(F.max("ingestion_date").alias("mature_cutoff"))
        .collect()[0]["mature_cutoff"]
    )


def processar(entity):
    cfg = CONFIG[entity]
    keys = cfg["chave"]
    bronze, silver, quarantine, history = paths_for(entity)

    # Contrato critical/high é pré-condição do runtime, inclusive em no-op.
    validator = CONTRACTS.validator(entity, keys)

    if not spark.catalog.tableExists(bronze):
        raise Exception(f"{entity}: Bronze ausente: {bronze}")
    if not spark.catalog.tableExists(silver):
        raise Exception(f"{entity}: Silver baseline ausente: {silver}")
    if not spark.catalog.tableExists(CONTROL_TABLE):
        raise Exception(f"Tabela de controle ausente: {CONTROL_TABLE}")

    states = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )
    if len(states) != 1:
        raise Exception(
            f"{entity}: esperada exatamente 1 linha de watermark; encontrado={len(states)}"
        )

    state = states[0]
    committed = state["last_processed_snapshot"]
    candidate = state["candidate_snapshot"]
    status = state["status"]

    if status == "PENDING_VALIDATION" and candidate is not None:
        print(
            f"ℹ️ {entity}: candidate={candidate} já está PENDING_VALIDATION. "
            "APPLY anterior preservado; seguindo para retry da validação sem novo MERGE."
        )
        return

    if status != "COMMITTED" or candidate is not None:
        raise Exception(
            f"{entity}: estado inicial inválido: committed={committed}, "
            f"candidate={candidate}, status={status}"
        )

    bronze_max = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]
    if bronze_max is None:
        raise Exception(f"{entity}: Bronze sem ingestion_date válido")

    mature_cutoff = latest_mature_partition(entity)
    if mature_cutoff is None:
        print(
            f"✅ {entity}: nenhuma partição madura disponível. "
            f"Bronze max visível={bronze_max}; no-op."
        )
        return

    if committed is not None and committed > mature_cutoff:
        raise Exception(
            f"{entity}: committed={committed} está à frente do mature_cutoff="
            f"{mature_cutoff}. Requer recuperação controlada antes de continuar."
        )

    pending = spark.table(bronze)
    if committed is not None:
        pending = pending.filter(F.col("ingestion_date") > F.lit(committed))
    pending = pending.filter(F.col("ingestion_date") <= F.lit(mature_cutoff))

    snapshots = [
        r["ingestion_date"]
        for r in (
            pending.select("ingestion_date").distinct()
            .orderBy("ingestion_date").collect()
        )
    ]

    print(f"\n=== FACT INCREMENTAL — {entity} ===")
    print(
        f"Committed: {committed} | Mature cutoff: {mature_cutoff} "
        f"| Bronze max visível: {bronze_max}"
    )
    print(f"Pending mature snapshots: {snapshots}")

    if not snapshots:
        print(f"✅ {entity}: nenhum snapshot pendente; no-op.")
        return

    transformed = aplicar_casts(pending, cfg)

    # Valida todo o lote. Unicidade é por grain+snapshot; só depois escolhemos
    # o último estado VÁLIDO por chave, preservando a semântica já aprovada.
    source, invalid, report = CONTRACTS.validate_snapshot_history(
        validator,
        transformed,
        keys,
    )
    CONTRACTS.log_report(entity, report)

    # Só promovemos o baseline de drift depois de o contrato estrutural passar.
    detectar_drift(entity, transformed)

    cond_merge = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (
        DeltaTable.forName(spark, silver).alias("t")
        .merge(source.alias("s"), cond_merge)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    if spark.catalog.tableExists(quarantine):
        spark.sql(f"TRUNCATE TABLE {quarantine}")

    invalid_count = invalid.count()
    if invalid_count:
        invalid.write.format("delta").mode("append").saveAsTable(quarantine)
        (
            invalid.withColumn("_quarantined_at", F.current_timestamp())
            .write.format("delta").mode("append").saveAsTable(history)
        )

    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET candidate_snapshot = DATE '{mature_cutoff}',
            status = 'PENDING_VALIDATION',
            updated_at = current_timestamp()
        WHERE entity = '{entity}'
          AND status = 'COMMITTED'
          AND candidate_snapshot IS NULL
    """)

    final_state = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()[0]
    )
    if (
        final_state["candidate_snapshot"] != mature_cutoff
        or final_state["status"] != "PENDING_VALIDATION"
    ):
        raise Exception(f"{entity}: falha ao registrar candidate_snapshot")

    print(
        f"✅ {entity}: APPLY concluído | snapshots={len(snapshots)} "
        f"| contract rows={report['total']:,} "
        f"| source final={source.count():,} | quarantine={invalid_count:,}"
    )
    print(
        f"✅ candidate={mature_cutoff}; committed continua={committed}; "
        "status=PENDING_VALIDATION"
    )
    print("ℹ️ Ausência de chave em snapshot não executa DELETE.")


entities = list(CONFIG) if ENTITY == "all" else [ENTITY]
for entity in entities:
    if entity not in CONFIG:
        raise Exception(f"Entidade não suportada: {entity}")
    processar(entity)

print("\n✅ APPLY incremental concluído. Nenhum watermark foi committed.")