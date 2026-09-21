# Databricks notebook source
# pipeline/silver/validate_mature_incremental_facts.py
# Validação batch-level para fatos incrementais com partições maduras.
#
# Semântica:
# - compara apenas o lote (committed, candidate]
# - exige que todas as chaves esperadas no lote existam na Silver com valores idênticos
# - permite chaves históricas extras na Silver (política no-delete)
# - falha se a Silver contiver qualquer linha com ingestion_date > candidate

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
        f"validate_mature_incremental_facts só pode executar em *_dev. Recebido: {CATALOG}"
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

    expected = filtrar_contrato(entity, aplicar_casts(batch, cfg))

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


entities = list(CONFIG) if ENTITY == "all" else [ENTITY]
for entity in entities:
    if entity not in CONFIG:
        raise Exception(f"Entidade não suportada: {entity}")
    validar(entity)

print("\n✅ Validação batch-level das partições maduras concluída.")
print("✅ Chaves históricas extras são permitidas pela política no-delete.")
print("✅ Nenhum watermark foi committed por esta task.")
