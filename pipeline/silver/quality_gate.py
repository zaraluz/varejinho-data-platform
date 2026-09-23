# Databricks notebook source
# pipeline/silver/quality_gate.py
# Quality Gate da Silver — valida maturidade incremental, contratos, SCD2 e quarentena.
# Falha com Exception se houver erros críticos.

from datetime import datetime, timedelta, timezone
import importlib.util
import os
import yaml

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
BRONZE_SOURCE_CATALOG = required_param("bronze_source_catalog")
BUNDLE_FILES_PATH = required_param("bundle_files_path").rstrip("/")
CONTROL_ROOT = required_param("control_root").rstrip("/")
FACT_WATERMARK = f"{CATALOG}.control.fact_watermark"
CONTRACT_POLICY = f"{BUNDLE_FILES_PATH}/contracts/silver/_policy.yaml"
CONTRACT_ENGINE = f"{BUNDLE_FILES_PATH}/quality/contract_engine.py"
resultados = []


def check(nome, passou, detalhe=""):
    status = "✅" if passou else "❌"
    resultados.append(f"{status} {nome} {detalhe}")


# ── Contract engine canônico ──────────────────────────────────────────────
if not os.path.isfile(CONTRACT_ENGINE):
    raise Exception(f"Contract engine ausente: {CONTRACT_ENGINE}")
if not os.path.isfile(CONTRACT_POLICY):
    raise Exception(f"Contract policy ausente: {CONTRACT_POLICY}")

spec = importlib.util.spec_from_file_location(
    "varejinho_contract_engine_qg",
    CONTRACT_ENGINE,
)
if spec is None or spec.loader is None:
    raise Exception(f"Não foi possível carregar contract engine: {CONTRACT_ENGINE}")
contract_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract_module)
ContractValidator = contract_module.ContractValidator
ContractViolation = contract_module.ContractViolation

PARTITION_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/partition_manifest_runtime.py"
partition_spec = importlib.util.spec_from_file_location(
    "varejinho_partition_manifest_runtime_qg",
    PARTITION_RUNTIME_PATH,
)
if partition_spec is None or partition_spec.loader is None:
    raise Exception(
        f"Não foi possível carregar partition manifest runtime: {PARTITION_RUNTIME_PATH}"
    )
partition_module = importlib.util.module_from_spec(partition_spec)
partition_spec.loader.exec_module(partition_module)
FactPartitionManifestRuntime = partition_module.FactPartitionManifestRuntime
MUTATION_GUARD = FactPartitionManifestRuntime(
    spark=spark,
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
    bundle_files_path=BUNDLE_FILES_PATH,
    bronze_source_catalog=BRONZE_SOURCE_CATALOG,
)

with open(CONTRACT_POLICY, "r", encoding="utf-8") as f:
    contract_policy = yaml.safe_load(f) or {}

required_contracts = []
standard_entities = []
for tier, cfg in (contract_policy.get("tiers", {}) or {}).items():
    entities = list(cfg.get("entities", []) or [])
    if bool(cfg.get("contract_required", False)):
        required_contracts.extend((entity, tier) for entity in entities)
    else:
        standard_entities.extend((entity, tier) for entity in entities)

if len(required_contracts) != 20 or len(standard_entities) != 17:
    raise Exception(
        "Contract policy inesperada: "
        f"required={len(required_contracts)} standard={len(standard_entities)}"
    )


# ── Fatos incrementais por partição madura ────────────────────────────────
# Venda e as 13 facts transacionais/financeiras usam o mesmo invariant diário:
# watermark único e COMMITTED, candidate limpo, committed == mature_cutoff
# e nenhuma linha da partição ainda aberta presente na Silver.

INCREMENTAL_FACTS = [
    "venda",
    "notaentrada",
    "notaentradaitem",
    "perda",
    "logestoque",
    "promocao",
    "promocaoitem",
    "pedido",
    "pedidoitem",
    "oferta",
    "pagarfornecedor",
    "pagarfornecedorparcela",
    "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto",
]

for tabela in INCREMENTAL_FACTS:
    try:
        source = f"{BRONZE_SOURCE_CATALOG}.bronze.{tabela}"

        maturity = spark.sql(f"""
            SELECT
                ingestion_date,
                MIN(to_date(_metadata.file_modification_time)) AS min_modified_date
            FROM {source}
            GROUP BY ingestion_date
        """)

        mature_cutoff = (
            maturity
            .filter(F.col("min_modified_date") > F.col("ingestion_date"))
            .agg(F.max("ingestion_date").alias("mature_cutoff"))
            .collect()[0]["mature_cutoff"]
        )

        states = (
            spark.table(FACT_WATERMARK)
            .filter(F.col("entity") == tabela)
            .collect()
        )

        check(
            f"{tabela} — watermark único",
            len(states) == 1,
            f"(rows de controle: {len(states)})",
        )

        if len(states) != 1:
            continue

        state = states[0]
        committed = state["last_processed_snapshot"]
        candidate = state["candidate_snapshot"]
        status = state["status"]

        state_ok = (
            mature_cutoff is not None
            and status == "COMMITTED"
            and candidate is None
            and committed == mature_cutoff
        )

        check(
            f"{tabela} — alinhado à partição madura",
            state_ok,
            f"(committed: {committed} | mature_cutoff: {mature_cutoff} | "
            f"candidate: {candidate} | status: {status})",
        )

        silver_max = (
            spark.table(f"{CATALOG}.silver.{tabela}")
            .agg(F.max("ingestion_date").alias("max_ingestion_date"))
            .collect()[0]["max_ingestion_date"]
        )

        no_future = (
            mature_cutoff is not None
            and (silver_max is None or silver_max <= mature_cutoff)
        )

        check(
            f"{tabela} — sem partição aberta na Silver",
            no_future,
            f"(Silver max ingestion_date: {silver_max} | mature_cutoff: {mature_cutoff})",
        )

        manifest_report = MUTATION_GUARD.assert_committed(
            tabela,
            committed,
        )
        check(
            f"{tabela} — histórico Bronze committed imutável",
            manifest_report["ok"],
            f"(manifest partitions: {manifest_report.get('manifest_rows', 0)})",
        )

    except Exception as e:
        resultados.append(f"❌ {tabela} incremental QG: {str(e)[:200]}")


# ── SCD2 — integridade das dimensões ──────────────────────────────────────
for dim in ["produto", "fornecedor", "mercadologico"]:
    try:
        multi = (
            spark.table(f"{CATALOG}.silver.{dim}")
            .filter("is_current = true")
            .groupBy("id")
            .count()
            .filter("count > 1")
            .count()
        )
        check(
            f"{dim} SCD2 — no máximo 1 versão ativa por id",
            multi == 0,
            f"({multi} ids com múltiplas versões ativas)",
        )

        nulos = (
            spark.table(f"{CATALOG}.silver.{dim}")
            .filter("is_current IS NULL")
            .count()
        )
        check(
            f"{dim} SCD2 — is_current não nulo",
            nulos == 0,
            f"({nulos} registros com is_current NULL)",
        )

    except Exception as e:
        resultados.append(f"❌ {dim} SCD2: {str(e)[:100]}")


# ── Data Contracts — gate estrutural central ──────────────────────────────
# Row-level error é aplicado no runtime e evidenciado pela quarentena abaixo.
# Aqui o QG evita rescan pesado das tabelas e prova que o contrato obrigatório
# continua compatível com o schema físico materializado na Silver.
for entity, tier in required_contracts:
    physical = f"{CATALOG}.silver.{entity}"
    contract_path = f"{BUNDLE_FILES_PATH}/contracts/silver/{entity}.yaml"
    try:
        if not spark.catalog.tableExists(physical):
            raise ContractViolation(f"Tabela Silver obrigatória ausente: {physical}")

        validator = ContractValidator(
            contract_path,
            spark=spark,
            catalog=CATALOG,
            schema="silver",
        )
        if validator.table != entity:
            raise ContractViolation(
                f"Contrato incorreto: esperado table={entity}; recebido={validator.table}"
            )

        # Usa a implementação de schema do engine canônico; não replica tipos aqui.
        validator._validate_dataframe_schema(spark.table(physical))
        check(
            f"{entity} — contract structural",
            True,
            f"(tier={tier}; schema compatível)",
        )
    except Exception as e:
        resultados.append(
            f"❌ {entity} contract structural: {str(e)[:220]}"
        )


# ── Standard — gate simplificado ──────────────────────────────────────────
# Essas 17 entidades não têm YAML completo por decisão arquitetural. O QG
# garante apenas disponibilidade estrutural; regras de fato não são inventadas.
for entity, tier in standard_entities:
    physical = f"{CATALOG}.silver.{entity}"
    try:
        exists = spark.catalog.tableExists(physical)
        col_count = len(spark.table(physical).columns) if exists else 0
        check(
            f"{entity} — standard availability",
            exists and col_count > 0,
            f"(tier={tier}; exists={exists}; columns={col_count})",
        )
    except Exception as e:
        resultados.append(
            f"❌ {entity} standard availability: {str(e)[:160]}"
        )


# ── Quarentena — valida somente a execução atual ──────────────────────────
QUARENTENAS = [
    "venda",
    "notaentrada",
    "notaentradaitem",
    "perda",
    "logestoque",
    "promocao",
    "promocaoitem",
    "pedido",
    "pedidoitem",
    "oferta",
    "pagarfornecedor",
    "pagarfornecedorparcela",
    "pagaroutrasdespesas",
    "pagaroutrasdespesasimposto",
]

for tabela in QUARENTENAS:
    try:
        quar_table = f"{CATALOG}.silver._quarantine_{tabela}"
        if spark.catalog.tableExists(quar_table):
            count = spark.table(quar_table).count()
            check(
                f"{tabela} — quarentena",
                count == 0,
                f"({count:,} registros rejeitados nesta execução)",
            )
    except Exception as e:
        resultados.append(f"❌ {tabela} quarentena: {str(e)[:100]}")


print(f"\n=== SILVER QUALITY GATE [{CATALOG}] ===\n")
for r in resultados:
    print(r)

total = len(resultados)
passou = sum(1 for r in resultados if r.startswith("✅"))
falhas = [r for r in resultados if r.startswith("❌")]
falhou = len(falhas)
print(f"\n{passou}/{total} checks passaram | {falhou} falharam")

if falhas:
    raise Exception(
        "Silver Quality Gate falhou:\n" + "\n".join(falhas)
    )
