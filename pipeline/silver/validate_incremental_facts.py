# Databricks notebook source
# pipeline/silver/validate_incremental_facts.py
# Valida Silver após APPLY incremental e antes do commit do fact_watermark.

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
BRONZE_OVERRIDE = job_param("bronze_table", "")
SILVER_OVERRIDE = job_param("silver_table", "")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"validate_incremental_facts só pode executar em *_dev. Recebido: {CATALOG}"
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


def filtrar_contrato(entity, df):
    path = f"{BUNDLE_FILES_PATH}/contracts/silver/{entity}.yaml"
    try:
        with open(path, "r") as f:
            contract = yaml.safe_load(f)
    except FileNotFoundError:
        return df

    work = df.withColumn("_invalido", F.lit(False))

    for cfg in contract.get("columns", []):
        name = cfg.get("name")
        if name not in work.columns:
            continue

        if not cfg.get("nullable", True):
            work = work.withColumn(
                "_invalido",
                F.when(F.col(name).isNull(), F.lit(True))
                 .otherwise(F.col("_invalido")),
            )

        min_val = cfg.get("min")
        if min_val is not None:
            try:
                min_num = float(min_val)
                work = work.withColumn(
                    "_invalido",
                    F.when(F.col(name).cast("double") < min_num, F.lit(True))
                     .otherwise(F.col("_invalido")),
                )
            except (TypeError, ValueError):
                pass

    return work.where(~F.col("_invalido")).drop("_invalido")


def validar(entity):
    cfg = CONFIG[entity]
    keys = cfg["chave"]
    bronze = BRONZE_OVERRIDE or f"{CATALOG}.bronze.{entity}"
    silver = SILVER_OVERRIDE or f"{CATALOG}.silver.{entity}"

    rows = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == entity)
        .collect()
    )
    if len(rows) != 1:
        raise Exception(f"{entity}: watermark esperado=1; encontrado={len(rows)}")

    row = rows[0]
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

    expected = (
        spark.table(bronze)
        .filter(F.col("ingestion_date") <= F.lit(candidate))
    )
    expected = filtrar_contrato(entity, aplicar_casts(expected, cfg))

    w = Window.partitionBy(*keys).orderBy(F.col("ingestion_date").desc())
    expected = (
        expected.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

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
            mismatches = joined.filter(diff_condition).count()
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
    print(f"schema exact:   {schema_ok}")
    print(f"rows:           expected={e_rows:,} | actual={a_rows:,}")
    print(f"duplicate keys: expected={e_dup:,} | actual={a_dup:,}")
    print(f"key coverage:   missing={missing:,} | extra={extra:,}")
    print(f"value mismatch: {mismatches}")
    print(f"RESULT:         {'✅ PASS' if ok else '❌ FAIL'}")

    if not ok:
        raise Exception(
            f"{entity}: incremental divergiu do full rebuild até {candidate}"
        )


entities = list(CONFIG) if ENTITY == "all" else [ENTITY]
for entity in entities:
    if entity not in CONFIG:
        raise Exception(f"Entidade não suportada: {entity}")
    validar(entity)

print("\n✅ Validação incremental concluída. Nenhum watermark foi committed.")
