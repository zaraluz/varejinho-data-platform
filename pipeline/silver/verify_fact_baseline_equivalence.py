# Databricks notebook source
# pipeline/silver/verify_fact_baseline_equivalence.py
# Gate D2 — prova que a Silver atual é exatamente o baseline produzido
# pela transformação full-historical vigente, antes de semear watermarks.
#
# Read-only: não grava Bronze, Silver, quarentena nem tabela de controle.

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
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D2 só pode executar em *_dev. Recebido: {CATALOG}")


# Mantém exatamente a configuração de transformação usada por transform_facts.py.
CONFIG = {
    "notaentrada": {
        "chave": ["numeronota", "id_loja", "id_fornecedor"],
        "data": "dataentrada",
        "decimais": ["valortotal", "valormercadoria", "valordesconto"],
    },
    "notaentradaitem": {
        "chave": ["id"],
        "data": None,
        "decimais": ["quantidade", "valor", "valortotal"],
    },
    "perda": {
        "chave": ["id"],
        "data": "data",
        "decimais": ["quantidade", "valor"],
    },
    "logestoque": {
        "chave": ["id"],
        "data": "datamovimento",
        "decimais": [
            "quantidade", "estoqueanterior", "estoqueatual",
            "custocomimposto", "custosemimposto",
            "customediocomimposto", "customediosemimposto",
        ],
    },
    "promocao": {
        "chave": ["id"],
        "data": "datainicio",
        "decimais": ["valor", "valordesconto"],
    },
    "promocaoitem": {
        "chave": ["id"],
        "data": None,
        "decimais": ["precovenda"],
    },
    "pedido": {
        "chave": ["id"],
        "data": "datacompra",
        "decimais": [],
    },
    "pedidoitem": {
        "chave": ["id"],
        "data": None,
        "decimais": ["quantidade", "custocompra", "valortotal"],
    },
    "oferta": {
        "chave": ["id"],
        "data": "datainicio",
        "decimais": ["precooferta", "preconormal"],
        "try_decimais": ["precoimediato"],
    },
    "pagarfornecedor": {
        "chave": ["id"],
        "data": "dataemissao",
        "decimais": ["valor"],
    },
    "pagarfornecedorparcela": {
        "chave": ["id"],
        "data": "datavencimento",
        "decimais": ["valor", "valoracrescimo"],
        "datas_extras": ["datapagamento", "datapagamentocontabil"],
    },
    "pagaroutrasdespesas": {
        "chave": ["id"],
        "data": "dataemissao",
        "decimais": ["valor", "valorbruto"],
    },
    "pagaroutrasdespesasimposto": {
        "chave": ["id"],
        "data": "datavencimento",
        "decimais": ["valor", "basecalculo", "aliquota"],
    },
}


def aplicar_contrato_readonly(tabela, df):
    """
    Reproduz a lógica de filtro de contrato de transform_facts.py sem gravar
    quarentena. Registros inválidos saem do expected exatamente como saem
    do caminho principal.
    """
    contract_path = f"{BUNDLE_FILES_PATH}/contracts/silver/{tabela}.yaml"

    try:
        with open(contract_path, "r") as f:
            contract = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[{tabela}] Contrato não encontrado — baseline segue sem filtro, igual ao runtime.")
        return df

    columns = contract.get("columns", [])
    work = df.withColumn("_invalido", F.lit(False))

    for col_cfg in columns:
        col_name = col_cfg.get("name")
        nullable = col_cfg.get("nullable", True)
        min_val = col_cfg.get("min", None)

        if col_name not in work.columns:
            continue

        if not nullable:
            work = work.withColumn(
                "_invalido",
                F.when(F.col(col_name).isNull(), F.lit(True))
                 .otherwise(F.col("_invalido")),
            )

        if min_val is not None:
            try:
                min_num = float(min_val)
                work = work.withColumn(
                    "_invalido",
                    F.when(F.col(col_name).cast("double") < min_num, F.lit(True))
                     .otherwise(F.col("_invalido")),
                )
            except (ValueError, TypeError):
                pass

    return work.where(~F.col("_invalido")).drop("_invalido")


def construir_expected(tabela, cfg):
    """
    Reconstrói em memória o estado Silver esperado a partir de TODA a Bronze,
    usando casts + contrato + deduplicação vigentes. Nenhuma escrita ocorre.
    """
    bronze = f"{CATALOG}.bronze.{tabela}"
    df = spark.table(bronze)

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

    for col_extra in cfg.get("datas_extras", []):
        if col_extra in df.columns:
            df = df.withColumn(
                col_extra,
                F.expr(
                    f"try_to_timestamp(`{col_extra}`, 'yyyy/MM/dd HH:mm:ss.SSS')"
                ),
            )

    df = aplicar_contrato_readonly(tabela, df)

    w = Window.partitionBy(*cfg["chave"]).orderBy(F.col("ingestion_date").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .where(F.col("_rn") == 1)
          .drop("_rn")
    )


def schema_map(df):
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


print("\n=== GATE D2 — FACT BASELINE EQUIVALENCE ===")
print(f"Catalog: {CATALOG}")
print("Expected = Bronze histórica transformada pelo runtime vigente.")
print("Actual   = Silver atual.")
print("Read-only: nenhum watermark será criado e nenhuma tabela será alterada.\n")

resultados = []

for tabela, cfg in CONFIG.items():
    print(f"\n{'=' * 92}")
    print(f"TABLE: {tabela}")

    expected = construir_expected(tabela, cfg)
    silver_name = f"{CATALOG}.silver.{tabela}"

    if not spark.catalog.tableExists(silver_name):
        print(f"❌ Silver ausente: {silver_name}")
        resultados.append((tabela, "FAIL", "silver_missing"))
        continue

    actual = spark.table(silver_name)
    keys = cfg["chave"]

    expected_schema = schema_map(expected)
    actual_schema = schema_map(actual)

    expected_cols = set(expected_schema)
    actual_cols = set(actual_schema)
    missing_cols = sorted(expected_cols - actual_cols)
    extra_cols = sorted(actual_cols - expected_cols)
    type_mismatches = sorted(
        [
            f"{c}: expected={expected_schema[c]} actual={actual_schema[c]}"
            for c in expected_cols & actual_cols
            if expected_schema[c] != actual_schema[c]
        ]
    )

    schema_ok = not missing_cols and not extra_cols and not type_mismatches

    expected_rows = expected.count()
    actual_rows = actual.count()

    expected_dup_keys = (
        expected.groupBy(*keys).count().filter(F.col("count") > 1).count()
    )
    actual_dup_keys = (
        actual.groupBy(*keys).count().filter(F.col("count") > 1).count()
    )

    expected_keys = expected.select(*keys)
    actual_keys = actual.select(*keys)

    missing_keys = expected_keys.join(actual_keys, on=keys, how="left_anti").count()
    extra_keys = actual_keys.join(expected_keys, on=keys, how="left_anti").count()

    value_mismatches = None

    if schema_ok and expected_dup_keys == 0 and actual_dup_keys == 0:
        common_cols = list(expected.columns)
        nonkey_cols = [c for c in common_cols if c not in keys]

        e = expected.select(*common_cols).alias("e")
        a = actual.select(*common_cols).alias("a")

        join_condition = reduce(
            lambda acc, key: acc & F.col(f"e.{key}").eqNullSafe(F.col(f"a.{key}")),
            keys[1:],
            F.col(f"e.{keys[0]}").eqNullSafe(F.col(f"a.{keys[0]}")),
        )

        joined = e.join(a, join_condition, how="inner")

        if nonkey_cols:
            diff_condition = reduce(
                lambda acc, col_name: acc
                | (~F.col(f"e.{col_name}").eqNullSafe(F.col(f"a.{col_name}"))),
                nonkey_cols[1:],
                ~F.col(f"e.{nonkey_cols[0]}").eqNullSafe(
                    F.col(f"a.{nonkey_cols[0]}")
                ),
            )
            value_mismatches = joined.filter(diff_condition).count()
        else:
            value_mismatches = 0

    ok = (
        schema_ok
        and expected_rows == actual_rows
        and expected_dup_keys == 0
        and actual_dup_keys == 0
        and missing_keys == 0
        and extra_keys == 0
        and value_mismatches == 0
    )

    print(f"Schema exact:              {'YES' if schema_ok else 'NO'}")
    if missing_cols:
        print(f"  missing columns:         {missing_cols}")
    if extra_cols:
        print(f"  extra columns:           {extra_cols}")
    if type_mismatches:
        print(f"  type mismatches:         {type_mismatches}")

    print(f"Rows:                      expected={expected_rows:,} | actual={actual_rows:,}")
    print(f"Duplicate keys:            expected={expected_dup_keys:,} | actual={actual_dup_keys:,}")
    print(f"Key coverage:              missing_in_actual={missing_keys:,} | extra_in_actual={extra_keys:,}")
    print(
        "Value mismatches:          "
        + ("n/a (schema/key issue)" if value_mismatches is None else f"{value_mismatches:,}")
    )
    print(f"RESULT:                    {'✅ PASS' if ok else '❌ FAIL'}")

    resultados.append(
        (
            tabela,
            "PASS" if ok else "FAIL",
            expected_rows,
            actual_rows,
            missing_keys,
            extra_keys,
            value_mismatches,
        )
    )

print("\n\n=== GATE D2 — SUMMARY ===")
print("table | status | expected_rows | actual_rows | missing_keys | extra_keys | value_mismatches")
for row in resultados:
    print(" | ".join("n/a" if v is None else str(v) for v in row))

falhas = [r for r in resultados if r[1] != "PASS"]

if falhas:
    raise Exception(
        "Gate D2 falhou; NÃO semear watermark. Tabelas divergentes: "
        + ", ".join(r[0] for r in falhas)
    )

print(f"\n✅ RESULTADO D2: {len(resultados)}/{len(CONFIG)} tabelas equivalentes.")
print("✅ Baseline Bronze-transformado == Silver atual para schema, chaves e valores.")
print("✅ Agora é seguro avançar para o desenho/seeding do fact_watermark.")
print("✅ Nenhuma tabela foi alterada.")
