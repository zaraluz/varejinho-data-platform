# Databricks notebook source
# pipeline/silver/incremental_facts.py
# Runtime incremental genérico Bronze -> Silver para fatos.
# APPLY somente: o watermark fica PENDING_VALIDATION até task de commit separada.

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json
import yaml


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
BRONZE_OVERRIDE = job_param("bronze_table", "")
SILVER_OVERRIDE = job_param("silver_table", "")
QUARANTINE_OVERRIDE = job_param("quarantine_table", "")
HISTORY_OVERRIDE = job_param("quarantine_history_table", "")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"incremental_facts só pode executar em *_dev durante hardening. Recebido: {CATALOG}"
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


def aplicar_contrato(tabela, df):
    contract_path = f"{BUNDLE_FILES_PATH}/contracts/silver/{tabela}.yaml"
    try:
        with open(contract_path, "r") as f:
            contract = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[{tabela}] Contrato não encontrado — seguindo sem filtro.")
        return df, df.where(F.lit(False))

    work = (
        df.withColumn("_invalido", F.lit(False))
          .withColumn("_motivo", F.lit(""))
    )

    for col_cfg in contract.get("columns", []):
        name = col_cfg.get("name")
        if name not in work.columns:
            continue

        if not col_cfg.get("nullable", True):
            work = (
                work.withColumn(
                    "_invalido",
                    F.when(F.col(name).isNull(), F.lit(True))
                     .otherwise(F.col("_invalido")),
                )
                .withColumn(
                    "_motivo",
                    F.when(
                        F.col(name).isNull(),
                        F.concat(F.col("_motivo"), F.lit(f"|{name} nulo")),
                    ).otherwise(F.col("_motivo")),
                )
            )

        min_val = col_cfg.get("min")
        if min_val is not None:
            try:
                min_num = float(min_val)
                work = (
                    work.withColumn(
                        "_invalido",
                        F.when(F.col(name).cast("double") < min_num, F.lit(True))
                         .otherwise(F.col("_invalido")),
                    )
                    .withColumn(
                        "_motivo",
                        F.when(
                            F.col(name).cast("double") < min_num,
                            F.concat(F.col("_motivo"), F.lit(f"|{name} < {min_val}")),
                        ).otherwise(F.col("_motivo")),
                    )
                )
            except (ValueError, TypeError):
                pass

    return (
        work.where(~F.col("_invalido")).drop("_invalido", "_motivo"),
        work.where(F.col("_invalido")).drop("_invalido"),
    )


def paths_for(entity):
    return (
        BRONZE_OVERRIDE or f"{CATALOG}.bronze.{entity}",
        SILVER_OVERRIDE or f"{CATALOG}.silver.{entity}",
        QUARANTINE_OVERRIDE or f"{CATALOG}.silver._quarantine_{entity}",
        HISTORY_OVERRIDE or f"{CATALOG}.silver._quarantine_history_{entity}",
    )


def processar(entity):
    cfg = CONFIG[entity]
    keys = cfg["chave"]
    bronze, silver, quarantine, history = paths_for(entity)

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

    if status != "COMMITTED" or candidate is not None:
        raise Exception(
            f"{entity}: estado inicial inválido: committed={committed}, "
            f"candidate={candidate}, status={status}"
        )

    bronze_max = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]
    if bronze_max is None:
        raise Exception(f"{entity}: Bronze sem ingestion_date válido")

    pending = spark.table(bronze)
    if committed is not None:
        pending = pending.filter(F.col("ingestion_date") > F.lit(committed))
    pending = pending.filter(F.col("ingestion_date") <= F.lit(bronze_max))

    snapshots = [
        r["ingestion_date"]
        for r in (
            pending.select("ingestion_date").distinct()
            .orderBy("ingestion_date").collect()
        )
    ]

    print(f"\n=== FACT INCREMENTAL — {entity} ===")
    print(f"Committed: {committed} | Bronze max: {bronze_max}")
    print(f"Pending snapshots: {snapshots}")

    if not snapshots:
        print(f"✅ {entity}: nenhum snapshot pendente; no-op.")
        return

    null_cond = None
    for key in keys:
        expr = F.col(key).isNull()
        null_cond = expr if null_cond is None else (null_cond | expr)
    null_keys = pending.filter(null_cond).count()
    if null_keys:
        raise Exception(f"{entity}: {null_keys} linha(s) pendente(s) com chave nula")

    dup_groups = (
        pending.groupBy(*(keys + ["ingestion_date"]))
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_groups:
        raise Exception(
            f"{entity}: {dup_groups} duplicidade(s) por chave/snapshot no lote pendente"
        )

    transformed = aplicar_casts(pending, cfg)
    detectar_drift(entity, transformed)
    valid, invalid = aplicar_contrato(entity, transformed)

    # Vários snapshots podem estar pendentes. A Silver é current-state:
    # basta o último estado VÁLIDO por chave dentro do lote.
    w = Window.partitionBy(*keys).orderBy(F.col("ingestion_date").desc())
    source = (
        valid.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

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
        SET candidate_snapshot = DATE '{bronze_max}',
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
        final_state["candidate_snapshot"] != bronze_max
        or final_state["status"] != "PENDING_VALIDATION"
    ):
        raise Exception(f"{entity}: falha ao registrar candidate_snapshot")

    print(
        f"✅ {entity}: APPLY concluído | snapshots={len(snapshots)} "
        f"| source final={source.count():,} | quarantine={invalid_count:,}"
    )
    print(
        f"✅ candidate={bronze_max}; committed continua={committed}; "
        "status=PENDING_VALIDATION"
    )
    print("ℹ️ Ausência de chave em snapshot não executa DELETE.")


entities = list(CONFIG) if ENTITY == "all" else [ENTITY]
for entity in entities:
    if entity not in CONFIG:
        raise Exception(f"Entidade não suportada: {entity}")
    processar(entity)

print("\n✅ APPLY incremental concluído. Nenhum watermark foi committed.")
