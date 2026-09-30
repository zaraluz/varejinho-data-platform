# Databricks notebook source
# ops/repair/backfill_notaentrada_history.py
# Reparo único: devolve à Silver o histórico de notaentrada que nunca chegou até ela.
#
# Por que existe: a carga histórica foi gravada na pasta do dia corrente da Bronze
# (ingestion_date=<hoje>/notaentrada.csv) e a extração diária sobrescreveu o mesmo
# arquivo antes da Silver ler (a regra D+1 protege contra mudança depois de D+1, não
# contra sobrescrita dentro de D). A versão recuperada do S3 foi guardada numa
# landing imutável própria, fora de bronze/notaentrada/, e registrada como tabela
# externa (runbook: docs/runbooks/notaentrada_history_repair.md). Voltar o arquivo
# para bronze/notaentrada/ quebraria o mutation guard (partição histórica nova).
#
# Fonte do reparo = landing do backfill ∪ Bronze diária já committed. A Bronze entra
# para devolver também lançamentos que o MERGE antigo por chave composta engoliu.
# Regra = a mesma do pipeline: a extração mais recente vence.
#   - id ausente na Silver            → insere
#   - id presente e fonte mais nova   → atualiza (s.ingestion_date > t.ingestion_date)
#   - id presente e Silver mais nova  → não mexe
# Conflito no mesmo dia: o arquivo recuperado e a pasta diária da Bronze têm a mesma
# data de extração (16/09). O diário é o mais recente dos dois (foi ele que sobrescreveu
# o backfill no S3), então, para um id presente nos dois, a linha do diário vence e a
# do backfill sai da fonte. Sem isso, o mesmo id apareceria duas vezes na mesma
# ingestion_date e o contrato (no_duplicates [id] por ingestion_date) quarentenaria.
#
# Um passo por execução (parâmetro `step`):
#   plan      só leitura: fontes, contrato, quantos inserts/updates, órfãos de item
#   apply     escreve (dry_run=true por padrão): inválidos no histórico de quarentena,
#             depois MERGE na Silver; registra em control.ops_repair_log as versões
#             antes/depois e o committed usado, para o verify reproduzir a mesma fonte
#   verify    só leitura: compara a versão anterior e a posterior ao apply
#   rollback  RESTORE da Silver para a versão anterior ao apply (exige confirmação)
#
# Travas: apply/rollback reais exigem confirm_target = catálogo e approved_by;
# watermark de notaentrada COMMITTED sem candidate; nenhum outro run ativo; a data
# do backfill não pode passar do committed (a Silver nunca recebe dado de partição
# que o pipeline ainda vai processar). Idempotente: um segundo apply não acha nada
# para inserir nem atualizar. Em prod roda como o service principal (run_as), dono
# da Silver.

import importlib.util
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def param(name: str, default: str | None = None) -> str:
    """Parâmetro do job; sem default, falta de valor falha na hora."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        value = ""
    if value:
        return value
    if default is None:
        raise ValueError(f"Parâmetro obrigatório ausente: '{name}'")
    return default


CATALOG = param("catalog")
BRONZE_SOURCE_CATALOG = param("bronze_source_catalog")
BUNDLE_FILES_PATH = param("bundle_files_path").rstrip("/")
STEP = param("step")
DRY_RUN = param("dry_run", "true").strip().lower() != "false"
CONFIRM = param("confirm_target", "-")
APPROVED_BY = param("approved_by", "-")
BACKFILL_TABLE = param("backfill_table")
BACKFILL_DATE = date.fromisoformat(param("backfill_ingestion_date"))
ROLLBACK_VERSION = param("rollback_version", "-")

ENTITY = "notaentrada"
KEY = ["id"]
SILVER = f"{CATALOG}.silver.{ENTITY}"
ITEMS = f"{CATALOG}.silver.notaentradaitem"
BRONZE = f"{BRONZE_SOURCE_CATALOG}.bronze.{ENTITY}"
HISTORY = f"{CATALOG}.silver._quarantine_history_{ENTITY}"
WATERMARK = f"{CATALOG}.control.fact_watermark"
REPAIR_LOG = f"{CATALOG}.control.ops_repair_log"   # 1 linha por apply real: versões e committed
REPAIR_NAME = "backfill_notaentrada_history"

# Mesma tipagem de CONFIG["notaentrada"] em pipeline/silver/incremental_facts.py.
# Quando a tipagem centralizada (quality/fact_typing.py) entrar na main, este reparo
# passa a importá-la; até lá, o verify compara o schema final com o da Silver e falha
# se as duas cópias divergirem.
DECIMAIS = ["valortotal", "valormercadoria", "valordesconto"]
DATA = "dataentrada"

STEPS = ["plan", "apply", "verify", "rollback"]
if STEP not in STEPS:
    raise ValueError(f"step inválido: {STEP}. Opções: {STEPS}")
if STEP in ("apply", "rollback") and not DRY_RUN:
    if CONFIRM != CATALOG:
        raise ValueError(
            f"Escrita bloqueada: confirm_target='{CONFIRM}'. "
            f"Para executar de verdade, passe confirm_target={CATALOG}."
        )
    if APPROVED_BY in ("", "-"):
        raise ValueError(f"{STEP} real exige approved_by")

PREFIX = "[dry-run] " if DRY_RUN and STEP in ("apply", "rollback") else ""
print(
    f"step={STEP} dry_run={DRY_RUN} catalog={CATALOG}\n"
    f"backfill: {BACKFILL_TABLE} (ingestion_date={BACKFILL_DATE}) | bronze: {BRONZE}\n"
)


def _load(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Não foi possível carregar: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONTRACTS = _load(
    "varejinho_contract_runtime_backfill", f"{BUNDLE_FILES_PATH}/quality/contract_runtime.py"
).SilverContractRuntime(spark=spark, catalog=CATALOG, bundle_files_path=BUNDLE_FILES_PATH)


# ── pré-condições ──────────────────────────────────────────────────────────

def committed_watermark() -> date:
    rows = spark.table(WATERMARK).filter(F.col("entity") == ENTITY).collect()
    if len(rows) != 1 or rows[0]["status"] != "COMMITTED" or rows[0]["candidate_snapshot"] is not None:
        estado = [(r["status"], r["candidate_snapshot"]) for r in rows]
        raise Exception(f"{ENTITY}: watermark precisa estar COMMITTED sem candidate; encontrado={estado}")
    committed = rows[0]["last_processed_snapshot"]
    if BACKFILL_DATE > committed:
        raise Exception(
            f"backfill_ingestion_date={BACKFILL_DATE} passa do committed={committed}: "
            "a Silver não pode receber dado de partição que o pipeline ainda vai processar"
        )
    print(f"✅ watermark {ENTITY}: COMMITTED em {committed}")
    return committed


def check_no_active_runs() -> None:
    from databricks.sdk import WorkspaceClient

    active = [
        r.run_name for r in WorkspaceClient().jobs.list_runs(active_only=True)
        if "Reparo do Histórico" not in (r.run_name or "")
    ]
    if active:
        raise Exception(f"Há runs ativos; o reparo precisa do pipeline parado: {active}")
    print("✅ nenhum outro run ativo no workspace")


# ── fonte: backfill ∪ Bronze committed, tipada como a Silver ───────────────

def typed(df):
    for col in DECIMAIS:
        df = df.withColumn(col, F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"))
    return (
        df.withColumn(DATA, F.to_timestamp(F.col(DATA), "yyyy/MM/dd HH:mm:ss.SSS"))
        .withColumn("ano", F.year(DATA))
        .withColumn("mes", F.month(DATA))
    )


def source(committed: date):
    silver_cols = spark.table(SILVER).columns
    # ingestion_date sai com o tipo que a Silver já tem: o backfill não tem pasta
    # ingestion_date=..., então a data vem do parâmetro (a data do arquivo recuperado).
    ing_type = spark.table(SILVER).schema["ingestion_date"].dataType

    backfill = (
        spark.table(BACKFILL_TABLE)
        .withColumn("ingestion_date", F.lit(BACKFILL_DATE.isoformat()).cast(ing_type))
    )
    bronze = (
        spark.table(BRONZE)
        .withColumn("ingestion_date", F.col("ingestion_date").cast(ing_type))
        .filter(F.col("ingestion_date").cast("date") <= F.lit(committed))
    )
    # Conflito no mesmo dia (ver cabeçalho): o diário vence, a linha do backfill sai.
    same_day_ids = bronze.filter(F.col("ingestion_date").cast("date") == F.lit(BACKFILL_DATE)).select("id")
    n_backfill = backfill.count()
    backfill = backfill.join(same_day_ids, "id", "left_anti")
    print(f"backfill: {n_backfill:,} linhas | {n_backfill - backfill.count():,} ids também na pasta "
          f"diária de {BACKFILL_DATE} (o diário vence; saem do backfill)")

    for name, df in (("backfill", backfill), ("bronze", bronze)):
        missing = sorted(set(silver_cols) - {"ano", "mes"} - set(df.columns))
        if missing:
            raise Exception(f"{name}: colunas da Silver ausentes na fonte: {missing}")

    # Projeção nas colunas da Silver: coluna a mais na fonte fica de fora, como a
    # projeção additive do pipeline; coluna a menos já falhou acima.
    union = typed(
        backfill.select(*[c for c in silver_cols if c not in ("ano", "mes")])
        .unionByName(bronze.select(*[c for c in silver_cols if c not in ("ano", "mes")]))
    ).select(*silver_cols)

    expected = {f.name: f.dataType for f in spark.table(SILVER).schema.fields}
    got = {f.name: f.dataType for f in union.schema.fields}
    diff = {c: (str(got.get(c)), str(t)) for c, t in expected.items() if got.get(c) != t}
    if diff:
        raise Exception(f"tipagem da fonte diverge da Silver (fonte, silver): {diff}")
    return union


def validated(committed: date):
    validator = CONTRACTS.validator(ENTITY, KEY)
    winners, invalid, report = CONTRACTS.validate_snapshot_history(validator, source(committed), KEY)
    CONTRACTS.log_report(ENTITY, report)
    return winners, invalid


def classify(winners, target):
    """Marca cada vencedor como insert, update ou noop contra `target` (uma versão da Silver)."""
    t = target.select(*KEY, F.col("ingestion_date").alias("_t_ing"))
    return (
        winners.join(t, KEY, "left")
        .withColumn(
            "_acao",
            F.when(F.col("_t_ing").isNull(), "insert")
            .when(F.col("ingestion_date") > F.col("_t_ing"), "update")
            .otherwise("noop"),
        )
        .drop("_t_ing")
    )


def orphan_items(headers) -> int:
    ids = headers.select(F.col("id").alias("id_notaentrada")).distinct()
    return (
        spark.table(ITEMS).select("id_notaentrada").distinct()
        .join(ids, "id_notaentrada", "left_anti").count()
    )


def current_version() -> int:
    return DeltaTable.forName(spark, SILVER).history(1).collect()[0]["version"]


def repair_commit():
    """Versões antes/depois e committed do último apply real, lidos do log de reparo.

    Log em tabela de controle, e não no userMetadata do commit: o userMetadata exige
    spark.conf.set, que o serverless não aceita para essa chave.
    """
    if not spark.catalog.tableExists(REPAIR_LOG):
        raise Exception(f"{REPAIR_LOG} não existe: o apply real não rodou")
    rows = (
        spark.table(REPAIR_LOG)
        .filter((F.col("repair") == REPAIR_NAME) & (F.col("target_table") == SILVER))
        .orderBy(F.col("applied_at").desc()).limit(1).collect()
    )
    if not rows:
        raise Exception(f"nenhum apply de {REPAIR_NAME} em {REPAIR_LOG}: o apply real não rodou")
    r = rows[0]
    return r["version_before"], r["version_after"], r["committed"]


# ── passos ─────────────────────────────────────────────────────────────────

if STEP == "plan" or STEP == "apply":
    committed = committed_watermark()
    if STEP == "apply" and not DRY_RUN:
        check_no_active_runs()

    winners, invalid = validated(committed)
    silver_now = spark.table(SILVER)
    # Sem .cache(): o serverless não aceita PERSIST. A fonte é recalculada a cada ação,
    # o que é seguro porque ela só lê partições imutáveis (backfill e Bronze committed).
    marked = classify(winners, silver_now)
    counts = {r["_acao"]: r["n"] for r in marked.groupBy("_acao").agg(F.count("*").alias("n")).collect()}
    n_insert, n_update, n_noop = counts.get("insert", 0), counts.get("update", 0), counts.get("noop", 0)
    n_invalid = invalid.count()

    after_ids = silver_now.select("id").unionByName(
        marked.filter(F.col("_acao") == "insert").select("id")
    )
    faixa = marked.agg(F.min(DATA).alias("min"), F.max(DATA).alias("max")).collect()[0]

    print(f"\n{PREFIX}=== {STEP.upper()} — {SILVER} ===")
    print(f"Silver hoje: {silver_now.count():,} linhas")
    print(f"Fonte válida (1 estado por id): insert={n_insert:,} | update={n_update:,} | sem mudança={n_noop:,}")
    print(f"Inválidos pelo contrato (vão para {HISTORY}): {n_invalid:,}")
    print(f"dataentrada da fonte: {faixa['min']} → {faixa['max']}")
    print(f"Itens sem cabeçalho: hoje={orphan_items(silver_now):,} | depois do apply={orphan_items(after_ids):,}")

    if STEP == "apply" and not DRY_RUN and n_insert + n_update == 0:
        # Idempotência: rodar de novo não regrava nada, nem a quarentena.
        print("\n✅ nada a inserir nem atualizar: o reparo já está aplicado. Nada foi gravado.")
    elif STEP == "apply" and not DRY_RUN:
        before = current_version()
        print(f"\nVersão da Silver ANTES do apply: {before}  ← rollback volta para esta")

        # Quarentena ANTES do MERGE: se o append falhar (schema), a Silver não mudou.
        if n_invalid:
            (
                invalid.withColumn("_quarantined_at", F.current_timestamp())
                .write.format("delta").mode("append").saveAsTable(HISTORY)
            )

        changes = marked.filter(F.col("_acao") != "noop").select(*silver_now.columns)
        (
            DeltaTable.forName(spark, SILVER).alias("t")
            .merge(changes.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll(condition="s.ingestion_date > t.ingestion_date")
            .whenNotMatchedInsertAll()
            .execute()
        )
        after = current_version()
        if after != before + 1:
            # Outro commit entrou entre o antes e o MERGE: o verify não teria como isolar o reparo.
            raise Exception(f"esperado 1 commit do MERGE ({before}→{before + 1}), encontrado {before}→{after}. "
                            f"Não rode o pipeline; rollback para {before} e investigue.")

        spark.createDataFrame(
            [(REPAIR_NAME, SILVER, before, after, committed, BACKFILL_TABLE, BACKFILL_DATE,
              n_insert, n_update, n_invalid, APPROVED_BY)],
            "repair string, target_table string, version_before long, version_after long, "
            "committed date, backfill_table string, backfill_ingestion_date date, "
            "inserted long, updated long, quarantined long, approved_by string",
        ).withColumn("applied_at", F.current_timestamp()).write.mode("append").saveAsTable(REPAIR_LOG)

        print(f"✅ apply concluído: versão da Silver agora = {after} | "
              f"inválidos no histórico de quarentena = {n_invalid:,} | aprovado por {APPROVED_BY}")
    elif STEP == "apply":
        print(f"\n{PREFIX}nada foi gravado. Para gravar: dry_run=false, confirm_target={CATALOG}, approved_by=<nome>")

elif STEP == "verify":
    # O apply confere que o MERGE foi um commit só (antes + 1 = depois): a versão
    # anterior é exatamente a Silver que o apply encontrou.
    v_before, v_after, committed = repair_commit()
    before = spark.read.option("versionAsOf", v_before).table(SILVER)
    after = spark.read.option("versionAsOf", v_after).table(SILVER)
    # Mesma fonte do apply: committed lido do commit, não o de hoje (o pipeline
    # pode ter avançado desde então). A Bronze até ele é imutável (mutation guard).
    print(f"fonte reconstruída com committed={committed} (o do apply)")
    winners, _ = validated(committed)

    ok = True

    def check(nome, passou, detalhe=""):
        global ok
        ok = ok and passou
        print(f"{'✅' if passou else '❌'} {nome} {detalhe}")

    n_before, n_after = before.count(), after.count()
    check("ids únicos depois", after.select("id").distinct().count() == n_after, f"({n_after:,} linhas)")

    # ids tocados = linhas que mudaram entre as versões. exceptAll compara NULL com
    # NULL como igual; um join em todas as colunas marcaria como tocada toda linha
    # com alguma coluna nula.
    touched = after.exceptAll(before).select("id").distinct()
    expected_ids = classify(winners, before).filter(F.col("_acao") != "noop").select("id")
    check(
        "ids tocados = inserts + updates previstos contra a versão anterior",
        touched.exceptAll(expected_ids).count() == 0 and expected_ids.exceptAll(touched).count() == 0,
    )

    # Linhas que o reparo não devia tocar continuam idênticas.
    untouched_before = before.join(touched, "id", "left_anti")
    untouched_after = after.join(touched, "id", "left_anti")
    check(
        "linhas fora do reparo inalteradas",
        untouched_before.exceptAll(untouched_after).count() == 0
        and untouched_after.exceptAll(untouched_before).count() == 0,
    )

    # Toda linha tocada é exatamente o estado vencedor da fonte.
    touched_rows = after.join(touched, "id", "left_semi")
    expected_rows = winners.join(touched, "id", "left_semi").select(*after.columns)
    check(
        "linhas tocadas = estado vencedor da fonte",
        touched_rows.exceptAll(expected_rows).count() == 0
        and expected_rows.exceptAll(touched_rows).count() == 0,
        f"({touched.count():,} ids inseridos ou atualizados)",
    )

    # Nenhum id vencedor ficou de fora.
    missing = winners.select("id").join(after.select("id"), "id", "left_anti").count()
    check("todo id válido da fonte está na Silver", missing == 0, f"(faltando: {missing:,})")

    # Nenhuma linha regrediu: para todo id, ingestion_date depois >= antes.
    regress = (
        after.select("id", F.col("ingestion_date").alias("d_after"))
        .join(before.select("id", F.col("ingestion_date").alias("d_before")), "id")
        .filter(F.col("d_after") < F.col("d_before")).count()
    )
    check("nenhuma linha voltou para uma extração mais antiga", regress == 0, f"({regress:,})")

    # Itens e cabeçalhos da Silver ATUAL: se o pipeline rodou depois do apply, os
    # itens novos têm cabeçalho só nas versões novas; comparar com `after` daria
    # órfão falso.
    orfaos = orphan_items(spark.table(SILVER))
    check("itens sem cabeçalho = 0 (Silver atual)", orfaos == 0, f"({orfaos:,})")

    print(f"\nlinhas: antes={n_before:,} depois={n_after:,} (+{n_after - n_before:,}) | versões {v_before}→{v_after}")
    if not ok:
        raise Exception("verify falhou; ver checks acima. Rollback: step=rollback com rollback_version=" + str(v_before))
    print("\n✅ verify concluído")

elif STEP == "rollback":
    if ROLLBACK_VERSION in ("", "-"):
        raise ValueError("rollback exige rollback_version (a versão ANTES do apply, impressa pelo apply)")
    print(f"{PREFIX}RESTORE TABLE {SILVER} TO VERSION AS OF {int(ROLLBACK_VERSION)}")
    if not DRY_RUN:
        check_no_active_runs()
        spark.sql(f"RESTORE TABLE {SILVER} TO VERSION AS OF {int(ROLLBACK_VERSION)}")
        print(f"✅ Silver restaurada para a versão {ROLLBACK_VERSION} (aprovado por {APPROVED_BY}). "
              f"Linhas gravadas em {HISTORY} pelo apply continuam lá (histórico).")
