# Databricks notebook source
# pipeline/silver/diagnose_fact_source_stability.py
# Gate D5A — diagnóstico read-only de divergência entre APPLY incremental e
# full rebuild esperado. Foco inicial: notaentradaitem.
#
# Objetivo: separar bug de MERGE/transformação de instabilidade da Bronze raw.

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import yaml


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "notaentradaitem")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/<USER>/varejinho-data-platform",
)
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.fact_watermark")

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate D5A só pode executar em *_dev. Recebido: {CATALOG}")

CONFIG = {
    "notaentradaitem": {
        "chave": ["id"],
        "data": None,
        "decimais": ["quantidade", "valor", "valortotal"],
    },
}


def aplicar_casts(df, cfg):
    for col in cfg["decimais"]:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"),
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


cfg = CONFIG[ENTITY]
keys = cfg["chave"]
bronze_name = f"{CATALOG}.bronze.{ENTITY}"
silver_name = f"{CATALOG}.silver.{ENTITY}"

state_rows = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("entity") == ENTITY)
    .collect()
)
if len(state_rows) != 1:
    raise Exception(f"{ENTITY}: esperado 1 watermark; encontrado={len(state_rows)}")

state = state_rows[0]
committed = state["last_processed_snapshot"]
candidate = state["candidate_snapshot"]
status = state["status"]

if status != "PENDING_VALIDATION" or candidate is None:
    raise Exception(
        f"{ENTITY}: D5A espera PENDING_VALIDATION; "
        f"committed={committed}, candidate={candidate}, status={status}"
    )

print("\n=== GATE D5A — FACT SOURCE STABILITY DIAGNOSTIC ===")
print(f"entity:    {ENTITY}")
print(f"committed: {committed}")
print(f"candidate: {candidate}")
print(f"status:    {status}")

bronze_raw = spark.table(bronze_name)
bronze_max = bronze_raw.agg(F.max("ingestion_date")).collect()[0][0]
print(f"Bronze max atual: {bronze_max}")

expected_base = (
    bronze_raw
    .filter(F.col("ingestion_date") <= F.lit(candidate))
)
expected = filtrar_contrato(ENTITY, aplicar_casts(expected_base, cfg))
w = Window.partitionBy(*keys).orderBy(F.col("ingestion_date").desc())
expected = (
    expected.withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)
actual = spark.table(silver_name)

expected_keys = expected.select(*keys)
actual_keys = actual.select(*keys)

missing = expected_keys.join(actual_keys, on=keys, how="left_anti")
extra = actual_keys.join(expected_keys, on=keys, how="left_anti")

missing_count = missing.count()
extra_count = extra.count()

print(f"\nExpected rows: {expected.count():,}")
print(f"Actual rows:   {actual.count():,}")
print(f"Missing keys:  {missing_count:,}")
print(f"Extra keys:    {extra_count:,}")

# Onde as chaves extras existem AGORA na Bronze?
bronze_all_keys = bronze_raw.select(*keys).distinct()
bronze_candidate_keys = (
    bronze_raw
    .filter(F.col("ingestion_date") <= F.lit(candidate))
    .select(*keys)
    .distinct()
)
bronze_after_candidate_keys = (
    bronze_raw
    .filter(F.col("ingestion_date") > F.lit(candidate))
    .select(*keys)
    .distinct()
)

extra_absent_all = extra.join(bronze_all_keys, on=keys, how="left_anti")
extra_present_all = extra.join(bronze_all_keys, on=keys, how="inner")
extra_present_candidate_raw = extra.join(
    bronze_candidate_keys, on=keys, how="inner"
)
extra_present_only_after = (
    extra.join(bronze_after_candidate_keys, on=keys, how="inner")
    .join(bronze_candidate_keys, on=keys, how="left_anti")
)

print("\n--- LOCALIZAÇÃO DAS CHAVES EXTRAS DA SILVER ---")
print(
    "Extras ausentes de TODA a Bronze atual: "
    f"{extra_absent_all.count():,}"
)
print(
    "Extras ainda presentes em alguma Bronze atual: "
    f"{extra_present_all.count():,}"
)
print(
    "Extras presentes em Bronze <= candidate (raw, antes do contrato): "
    f"{extra_present_candidate_raw.count():,}"
)
print(
    "Extras presentes somente depois do candidate: "
    f"{extra_present_only_after.count():,}"
)

print("\nAmostra de extras totalmente ausentes da Bronze atual:")
extra_absent_all.limit(20).show(truncate=False)

print("\nAmostra de extras que ainda aparecem na Bronze atual:")
(
    bronze_raw.join(extra_present_all.limit(20), on=keys, how="inner")
    .orderBy(*keys, "ingestion_date")
    .show(100, truncate=False)
)

print("\n--- CHAVES ESPERADAS MAS AUSENTES NA SILVER ---")
missing.limit(20).show(truncate=False)

if missing_count:
    print("\nOcorrências atuais na Bronze das missing keys:")
    (
        bronze_raw.join(missing.limit(20), on=keys, how="inner")
        .orderBy(*keys, "ingestion_date")
        .show(100, truncate=False)
    )

    missing_dates = (
        bronze_raw.join(missing, on=keys, how="inner")
        .groupBy("ingestion_date")
        .count()
        .orderBy("ingestion_date")
    )
    print("\nDistribuição das missing keys por ingestion_date:")
    missing_dates.show(100, truncate=False)

# A distribuição de arquivos ajuda a detectar substituição/late write.
# Nem todo runtime UC expõe input_file_name em views; falha é informativa e não
# invalida o restante do diagnóstico.
try:
    source_with_file = bronze_raw.withColumn(
        "_source_file", F.input_file_name()
    )

    if missing_count:
        print("\nArquivos-fonte das missing keys (quando disponível):")
        (
            source_with_file.join(missing.limit(20), on=keys, how="inner")
            .select(*keys, "ingestion_date", "_source_file")
            .orderBy(*keys, "ingestion_date")
            .show(100, truncate=False)
        )

    if extra_present_all.count():
        print("\nArquivos-fonte das extras ainda presentes (quando disponível):")
        (
            source_with_file.join(
                extra_present_all.limit(20), on=keys, how="inner"
            )
            .select(*keys, "ingestion_date", "_source_file")
            .orderBy(*keys, "ingestion_date")
            .show(100, truncate=False)
        )
except Exception as exc:
    print(
        "\nℹ️ Metadado de arquivo não disponível neste objeto Bronze: "
        f"{str(exc)[:500]}"
    )

print("\n--- INTERPRETAÇÃO AUTOMÁTICA ---")
absent_all_count = extra_absent_all.count()
missing_on_candidate = (
    bronze_raw.join(missing, on=keys, how="inner")
    .filter(F.col("ingestion_date") == F.lit(candidate))
    .select(*keys)
    .distinct()
    .count()
    if missing_count
    else 0
)

if absent_all_count > 0:
    print(
        "⚠️ Existem chaves na Silver que não existem mais em NENHUM arquivo "
        "Bronze atualmente visível. Isso é evidência de mutabilidade/remoção "
        "na camada raw após o baseline/APPLY."
    )

if missing_on_candidate > 0:
    print(
        "⚠️ Existem chaves válidas no próprio candidate que não chegaram à "
        "Silver durante o APPLY. Se o candidate já havia sido fechado quando "
        "o APPLY rodou, isso é compatível com late write/mutação dentro da "
        "mesma ingestion_date."
    )

if absent_all_count == 0 and missing_on_candidate == 0:
    print(
        "ℹ️ O padrão não prova mutabilidade da Bronze sozinho; investigar "
        "contrato/semântica das chaves presentes em raw mas filtradas."
    )

print("\n✅ Gate D5A é read-only. Nenhum dado ou watermark foi alterado.")
