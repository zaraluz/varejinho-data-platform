# Databricks notebook source
# ops/repair/validate_fact_maturity_repair.py
# Gate D6B — valida one-time repair antes de corrigir o committed watermark.

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import yaml


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
ENTITY = job_param("entity", "")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")
REPAIR_FROM = job_param("repair_from", "2026-09-18")

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

if not CATALOG.endswith("_dev") or ENTITY not in CONFIG:
    raise Exception(f"D6B validator inválido: catalog={CATALOG} entity={ENTITY}")


def casts(df, cfg):
    for col in cfg["decimais"]:
        if col in df.columns:
            df = df.withColumn(col, F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"))
    for col in cfg.get("try_decimais", []):
        if col in df.columns:
            df = df.withColumn(col, F.expr(f"try_cast(replace(`{col}`, ',', '.') as decimal(14,3))"))
    if cfg["data"] and cfg["data"] in df.columns:
        df = (df.withColumn(cfg["data"], F.to_timestamp(F.col(cfg["data"]), "yyyy/MM/dd HH:mm:ss.SSS"))
                .withColumn("ano", F.year(cfg["data"]))
                .withColumn("mes", F.month(cfg["data"])))
    for col in cfg.get("datas_extras", []):
        if col in df.columns:
            df = df.withColumn(col, F.expr(f"try_to_timestamp(`{col}`, 'yyyy/MM/dd HH:mm:ss.SSS')"))
    return df


def contract(entity, df):
    path = f"{BUNDLE_FILES_PATH}/contracts/silver/{entity}.yaml"
    try:
        with open(path, "r") as f:
            cfgs = yaml.safe_load(f).get("columns", [])
    except FileNotFoundError:
        return df

    work = df.withColumn("_invalido", F.lit(False))
    for cfg in cfgs:
        name = cfg.get("name")
        if name not in work.columns:
            continue
        if not cfg.get("nullable", True):
            work = work.withColumn("_invalido", F.when(F.col(name).isNull(), True).otherwise(F.col("_invalido")))
        if cfg.get("min") is not None:
            try:
                min_num = float(cfg["min"])
                work = work.withColumn("_invalido", F.when(F.col(name).cast("double") < min_num, True).otherwise(F.col("_invalido")))
            except (TypeError, ValueError):
                pass
    return work.where(~F.col("_invalido")).drop("_invalido")


cfg = CONFIG[ENTITY]
keys = cfg["chave"]
bronze = f"{CATALOG}.bronze.{ENTITY}"
silver = f"{CATALOG}.silver.{ENTITY}"

state = spark.table(CONTROL_TABLE).filter(F.col("entity") == ENTITY).collect()[0]
candidate = state["candidate_snapshot"]
status = state["status"]

if status != "REPAIR_PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"{ENTITY}: estado inválido para repair validation: candidate={candidate} status={status}"
    )

expected = contract(
    ENTITY,
    casts(
        spark.table(bronze)
        .filter(F.col("ingestion_date") >= F.lit(REPAIR_FROM))
        .filter(F.col("ingestion_date") <= F.lit(candidate)),
        cfg,
    ),
)

w = Window.partitionBy(*keys).orderBy(F.col("ingestion_date").desc())
expected = expected.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
actual = spark.table(silver)

e_schema = {f.name: f.dataType.simpleString() for f in expected.schema.fields}
a_schema = {f.name: f.dataType.simpleString() for f in actual.schema.fields}
schema_ok = e_schema == a_schema

e_keys = expected.select(*keys)
a_keys = actual.select(*keys)
missing = e_keys.join(a_keys, on=keys, how="left_anti").count()
extra = a_keys.join(e_keys, on=keys, how="left_anti").count()
a_dup = actual.groupBy(*keys).count().filter("count > 1").count()
future = actual.filter(F.col("ingestion_date") > F.lit(candidate)).count()

mismatches = None
if schema_ok and a_dup == 0:
    cols = expected.columns
    nonkeys = [c for c in cols if c not in keys]
    e = expected.alias("e")
    a = actual.alias("a")
    join_cond = reduce(
        lambda acc, k: acc & F.col(f"e.{k}").eqNullSafe(F.col(f"a.{k}")),
        keys[1:],
        F.col(f"e.{keys[0]}").eqNullSafe(F.col(f"a.{keys[0]}")),
    )
    joined = e.join(a, join_cond, "inner")
    if nonkeys:
        diff = reduce(
            lambda acc, c: acc | (~F.col(f"e.{c}").eqNullSafe(F.col(f"a.{c}"))),
            nonkeys[1:],
            ~F.col(f"e.{nonkeys[0]}").eqNullSafe(F.col(f"a.{nonkeys[0]}")),
        )
        mismatches = joined.filter(diff).count()
    else:
        mismatches = 0

ok = schema_ok and missing == 0 and a_dup == 0 and future == 0 and mismatches == 0

print(f"\n=== D6B REPAIR VALIDATE — {ENTITY} ===")
print(f"repair window: {REPAIR_FROM} -> {candidate}")
print(f"schema exact: {schema_ok}")
print(f"expected repair-window keys: {expected.count():,}")
print(f"silver rows: {actual.count():,}")
print(f"missing expected keys: {missing:,}")
print(f"historical extras allowed: {extra:,}")
print(f"duplicate keys: {a_dup:,}")
print(f"rows > candidate: {future:,}")
print(f"value mismatches on expected keys: {mismatches}")
print(f"RESULT: {'✅ PASS' if ok else '❌ FAIL'}")

if not ok:
    raise Exception(f"{ENTITY}: maturity repair validation falhou")

print("✅ Repair validado. Extras históricos foram preservados pela política no-delete.")
