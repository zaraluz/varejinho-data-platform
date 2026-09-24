# Databricks notebook source
# ops/cutover/production_cutover.py
# Cutover de produção: backup do legado, clone do estado validado de dev e verificação.
#
# Um passo por execução (parâmetro `step`), nesta ordem:
#   plan       só leitura: checagens prévias e o que os passos seguintes fariam
#   backup     Silver/Gold legadas de prod → <schema>_legacy (DEEP CLONE);
#              controle legado no S3 → legacy_control_root
#   clone      Silver e control de dev → prod (DEEP CLONE); estado de controle
#              validado no S3 (schema_registry, fact_partition_manifest)
#   verify     dev × prod: tabelas, schema, contagem e checksum; arquivos do S3
#   ownership  tabelas de prod (silver, gold, control) passam para o service principal
#   rollback   devolve Silver/Gold e o controle legados (antes do primeiro run de prod)
#
# Travas:
#   - dry_run=true por padrão: imprime cada comando sem executar;
#   - qualquer escrita exige confirm_target igual ao catálogo de destino;
#   - backup recusa rodar se já existir backup ou se o clone já aconteceu,
#     para nunca sobrescrever o legado com o estado novo.
#
# A Gold não é clonada: o primeiro run de prod a reconstrói a partir da Silver.
# Este job existe só no target dev e roda como a operadora, que acessa os dois
# catálogos; o service principal de prod não tem acesso a dev.

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


STEP = param("step")
DRY_RUN = param("dry_run", "true").strip().lower() != "false"
CONFIRM = param("confirm_target", "-")
SOURCE = param("source_catalog")
TARGET = param("target_catalog")
SOURCE_CONTROL = param("source_control_root").rstrip("/")
TARGET_CONTROL = param("target_control_root").rstrip("/")
LEGACY_CONTROL = param("legacy_control_root").rstrip("/")
SERVICE_PRINCIPAL = param("service_principal", "-")
SKIP_RUN_CHECK = param("skip_active_run_check", "false").strip().lower() == "true"

STEPS = ["plan", "backup", "clone", "verify", "ownership", "rollback"]
CLONED_SCHEMAS = ["silver", "control"]                 # dev → prod
LEGACY_SCHEMAS = {"silver": "silver_legacy", "gold": "gold_legacy"}
CONTROL_DIRS = ["schema_registry", "fact_partition_manifest"]   # estado real em dev
# Legado do pipeline antigo na raiz de prod: sai antes do clone porque o
# schema_registry novo usa o mesmo caminho.
LEGACY_CONTROL_DIRS = ["schema_registry"]
# watermark_backup/ também está na raiz de prod, mas não é legado: a extração
# on-premises grava um backup por dia ali, fora do Databricks. O cutover não toca.
WATERMARK_TABLES = ["fact_watermark", "scd2_watermark"]

if STEP not in STEPS:
    raise ValueError(f"step inválido: {STEP}. Opções: {STEPS}")
if SOURCE == TARGET:
    raise ValueError("source_catalog e target_catalog precisam ser diferentes")
if not SOURCE.endswith("_dev") or TARGET.endswith("_dev"):
    raise ValueError(f"Direção inválida: {SOURCE} → {TARGET} (esperado dev → prod)")
if SOURCE_CONTROL == TARGET_CONTROL or not SOURCE_CONTROL.startswith(TARGET_CONTROL + "/"):
    raise ValueError("source_control_root deve ser uma subpasta de target_control_root")
if not DRY_RUN and STEP not in ("plan", "verify") and CONFIRM != TARGET:
    raise ValueError(
        f"Escrita bloqueada: confirm_target='{CONFIRM}'. "
        f"Para executar de verdade, passe confirm_target={TARGET}."
    )

PREFIX = "[dry-run] " if DRY_RUN else ""
print(f"step={STEP} dry_run={DRY_RUN} {SOURCE} → {TARGET}")
print(f"control: {SOURCE_CONTROL} → {TARGET_CONTROL} (legado → {LEGACY_CONTROL})")


# ── utilidades ─────────────────────────────────────────────────────────────

def run(sql: str) -> None:
    print(f"{PREFIX}{sql}")
    if not DRY_RUN:
        spark.sql(sql)


def schema_exists(catalog: str, schema: str) -> bool:
    return spark.sql(
        f"SELECT 1 FROM {catalog}.information_schema.schemata "
        f"WHERE schema_name = '{schema}'"
    ).count() > 0


def tables(catalog: str, schema: str) -> list[str]:
    """Tabelas (não views) de um schema; lista vazia se o schema não existe."""
    if not schema_exists(catalog, schema):
        return []
    rows = spark.sql(
        f"SELECT table_name FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_type IN ('MANAGED', 'EXTERNAL') "
        f"ORDER BY table_name"
    ).collect()
    return [r["table_name"] for r in rows]


def list_files(root: str) -> dict[str, int]:
    """Arquivos sob root: caminho relativo → tamanho. Vazio se root não existe."""
    root = root.rstrip("/")
    try:
        pending = list(dbutils.fs.ls(root))
    except Exception as exc:
        if "FileNotFound" in str(exc) or "does not exist" in str(exc):
            return {}
        raise
    files = {}
    while pending:
        entry = pending.pop()
        if entry.name.endswith("/"):
            pending.extend(dbutils.fs.ls(entry.path))
        else:
            files[entry.path[len(root) + 1:]] = entry.size
    return files


def copy_tree(src: str, dst: str) -> dict[str, int]:
    """Copia src → dst e confere nome e tamanho de cada arquivo."""
    files = list_files(src)
    print(f"{PREFIX}cp -r {src} → {dst} ({len(files)} arquivos)")
    if not DRY_RUN:
        dbutils.fs.cp(src, dst, recurse=True)
        copied = list_files(dst)
        diff = sorted(k for k, size in files.items() if copied.get(k) != size)
        if diff:
            raise Exception(f"Cópia incompleta {src} → {dst}: {diff[:10]}")
    return files


def move_tree(src: str, dst: str) -> None:
    """Move = copia, confere e só então apaga a origem."""
    copy_tree(src, dst)
    print(f"{PREFIX}rm -r {src}")
    if not DRY_RUN:
        dbutils.fs.rm(src, recurse=True)


def checksum(table: str) -> tuple[int, int]:
    """(linhas, soma de xxhash64 de todas as colunas): igual ⇒ mesmo conteúdo, na prática."""
    df = spark.table(table)
    row = df.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.xxhash64(*[F.col(f"`{c}`") for c in df.columns]).cast("decimal(38,0)")).alias("h"),
    ).collect()[0]
    return row["n"], row["h"]


def columns(table: str) -> list[tuple]:
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in spark.table(table).schema]


# ── checagens ──────────────────────────────────────────────────────────────

def check_no_active_runs() -> None:
    if SKIP_RUN_CHECK:
        print("⚠️ checagem de runs ativos pulada (skip_active_run_check=true)")
        return
    from databricks.sdk import WorkspaceClient

    active = [
        r.run_name for r in WorkspaceClient().jobs.list_runs(active_only=True)
        if "Cutover" not in (r.run_name or "")
    ]
    if active:
        raise Exception(f"Há runs ativos; o clone precisa de dev parado: {active}")
    print("✅ nenhum outro run ativo no workspace")


def check_source_watermarks() -> None:
    for name in WATERMARK_TABLES:
        wm = spark.table(f"{SOURCE}.control.{name}")
        bad = wm.filter("status <> 'COMMITTED' OR candidate_snapshot IS NOT NULL")
        if bad.count():
            bad.show(truncate=False)
            raise Exception(f"{SOURCE}.control.{name} tem entidade fora de COMMITTED")
        print(f"✅ {SOURCE}.control.{name}: {wm.count()} entidades COMMITTED")


def check_backup_absent() -> None:
    for legacy in LEGACY_SCHEMAS.values():
        existing = tables(TARGET, legacy)
        if existing:
            raise Exception(
                f"{TARGET}.{legacy} já tem {len(existing)} tabelas. O backup é único: "
                "confira e apague esse schema manualmente antes de repetir."
            )
    if tables(TARGET, "control"):
        raise Exception(f"{TARGET}.control já existe com tabelas: o clone já aconteceu")
    if list_files(LEGACY_CONTROL):
        raise Exception(f"{LEGACY_CONTROL} já tem arquivos: o backup do controle já aconteceu")


def check_backup_complete() -> None:
    for schema, legacy in LEGACY_SCHEMAS.items():
        if not tables(TARGET, legacy):
            raise Exception(f"Sem backup em {TARGET}.{legacy}: rode step=backup antes")
        # Antes do primeiro clone, toda tabela de prod precisa ter cópia no backup.
        # Depois dele (schema control já existe), prod já contém o estado novo.
        missing = sorted(set(tables(TARGET, schema)) - set(tables(TARGET, legacy)))
        if missing and not tables(TARGET, "control"):
            raise Exception(f"Backup incompleto: {TARGET}.{legacy} sem {missing}")
    leftovers = [d for d in LEGACY_CONTROL_DIRS if list_files(f"{TARGET_CONTROL}/{d}")
                 and d not in CONTROL_DIRS]
    if leftovers:
        raise Exception(f"Controle legado ainda na raiz de prod: {leftovers}")
    print("✅ backup do legado presente")


# ── passos ─────────────────────────────────────────────────────────────────

def step_plan() -> None:
    check_no_active_runs()
    check_source_watermarks()
    for schema in CLONED_SCHEMAS:
        print(f"• {SOURCE}.{schema}: {len(tables(SOURCE, schema))} tabelas para clonar")
    for schema, legacy in LEGACY_SCHEMAS.items():
        print(f"• {TARGET}.{schema}: {len(tables(TARGET, schema))} tabelas → backup em {legacy}")
    extra = sorted(set(tables(TARGET, "silver")) - set(tables(SOURCE, "silver")))
    print(f"• só em {TARGET}.silver (saem depois do backup): {extra}")
    for d in CONTROL_DIRS:
        print(f"• {SOURCE_CONTROL}/{d}: {len(list_files(f'{SOURCE_CONTROL}/{d}'))} arquivos")
    for d in LEGACY_CONTROL_DIRS:
        print(f"• legado {TARGET_CONTROL}/{d}: {len(list_files(f'{TARGET_CONTROL}/{d}'))} arquivos")
    check_backup_absent()
    print("✅ plan: pronto para backup")


def step_backup() -> None:
    check_no_active_runs()
    check_backup_absent()
    for schema, legacy in LEGACY_SCHEMAS.items():
        run(f"CREATE SCHEMA IF NOT EXISTS {TARGET}.{legacy}")
        for t in tables(TARGET, schema):
            run(f"CREATE TABLE {TARGET}.{legacy}.{t} DEEP CLONE {TARGET}.{schema}.{t}")
            copy, original = f"{TARGET}.{legacy}.{t}", f"{TARGET}.{schema}.{t}"
            if not DRY_RUN and checksum(copy) != checksum(original):
                raise Exception(f"Backup divergente: {copy}")
    for d in LEGACY_CONTROL_DIRS:
        if list_files(f"{TARGET_CONTROL}/{d}"):
            move_tree(f"{TARGET_CONTROL}/{d}", f"{LEGACY_CONTROL}/{d}")
    print("✅ backup concluído" if not DRY_RUN else "✅ backup (dry-run) listado")


def step_clone() -> None:
    check_no_active_runs()
    check_source_watermarks()
    check_backup_complete()
    run(f"CREATE SCHEMA IF NOT EXISTS {TARGET}.control")
    for schema in CLONED_SCHEMAS:
        for t in tables(SOURCE, schema):
            run(f"CREATE OR REPLACE TABLE {TARGET}.{schema}.{t} DEEP CLONE {SOURCE}.{schema}.{t}")
    for t in sorted(set(tables(TARGET, "silver")) - set(tables(SOURCE, "silver"))):
        if t not in tables(TARGET, LEGACY_SCHEMAS["silver"]):
            raise Exception(f"{TARGET}.silver.{t} não está no backup; não será apagada")
        run(f"DROP TABLE {TARGET}.silver.{t}")
    for d in CONTROL_DIRS:
        existing = list_files(f"{TARGET_CONTROL}/{d}")
        if existing and existing != list_files(f"{SOURCE_CONTROL}/{d}"):
            raise Exception(f"{TARGET_CONTROL}/{d} já tem arquivos diferentes de dev")
        copy_tree(f"{SOURCE_CONTROL}/{d}", f"{TARGET_CONTROL}/{d}")
    print("✅ clone concluído: rode step=verify" if not DRY_RUN else "✅ clone (dry-run) listado")


def step_verify() -> None:
    failures = []
    for schema in CLONED_SCHEMAS:
        src, dst = tables(SOURCE, schema), tables(TARGET, schema)
        if src != dst:
            failures.append(f"{schema}: tabelas diferentes {sorted(set(src) ^ set(dst))}")
        for t in sorted(set(src) & set(dst)):
            a, b = f"{SOURCE}.{schema}.{t}", f"{TARGET}.{schema}.{t}"
            if columns(a) != columns(b):
                failures.append(f"{b}: schema diferente")
                continue
            ca, cb = checksum(a), checksum(b)
            status = "✅" if ca == cb else "❌"
            print(f"{status} {b}: {cb[0]} linhas")
            if ca != cb:
                failures.append(f"{b}: conteúdo diferente ({ca[0]} × {cb[0]} linhas)")
    for d in CONTROL_DIRS:
        src, dst = list_files(f"{SOURCE_CONTROL}/{d}"), list_files(f"{TARGET_CONTROL}/{d}")
        status = "✅" if src == dst and src else "❌"
        print(f"{status} {TARGET_CONTROL}/{d}: {len(dst)} arquivos")
        if src != dst or not src:
            failures.append(f"{d}: arquivos diferentes ou ausentes")
    for legacy in LEGACY_SCHEMAS.values():
        print(f"• backup {TARGET}.{legacy}: {len(tables(TARGET, legacy))} tabelas")
    if failures:
        raise Exception("verify falhou:\n" + "\n".join(failures))
    print("✅ verify: prod idêntico ao estado validado de dev")


def step_ownership() -> None:
    if SERVICE_PRINCIPAL in ("", "-"):
        raise ValueError("service_principal obrigatório para ownership")
    for schema in ["silver", "gold", "control"]:
        for t in tables(TARGET, schema):
            run(f"ALTER TABLE {TARGET}.{schema}.{t} OWNER TO `{SERVICE_PRINCIPAL}`")
    if DRY_RUN:
        print("✅ ownership (dry-run) listada")
        return
    # A operadora é dona do catálogo e dos schemas; confirma que continua lendo.
    for schema in ["silver", "gold", "control"]:
        first = tables(TARGET, schema)[0]
        spark.table(f"{TARGET}.{schema}.{first}").limit(1).count()
    print("✅ ownership transferida; leitura da operadora confirmada")


def step_rollback() -> None:
    for schema, legacy in LEGACY_SCHEMAS.items():
        backup = tables(TARGET, legacy)
        if not backup:
            raise Exception(f"Sem backup em {TARGET}.{legacy}: rollback impossível")
        for t in backup:
            run(f"CREATE OR REPLACE TABLE {TARGET}.{schema}.{t} DEEP CLONE {TARGET}.{legacy}.{t}")
        for t in sorted(set(tables(TARGET, schema)) - set(backup)):
            run(f"DROP TABLE {TARGET}.{schema}.{t}")
    run(f"DROP SCHEMA IF EXISTS {TARGET}.control CASCADE")
    for d in CONTROL_DIRS:
        print(f"{PREFIX}rm -r {TARGET_CONTROL}/{d}")
        if not DRY_RUN:
            dbutils.fs.rm(f"{TARGET_CONTROL}/{d}", recurse=True)
    for d in LEGACY_CONTROL_DIRS:
        if list_files(f"{LEGACY_CONTROL}/{d}"):
            copy_tree(f"{LEGACY_CONTROL}/{d}", f"{TARGET_CONTROL}/{d}")
    print("✅ rollback concluído" if not DRY_RUN else "✅ rollback (dry-run) listado")


{
    "plan": step_plan,
    "backup": step_backup,
    "clone": step_clone,
    "verify": step_verify,
    "ownership": step_ownership,
    "rollback": step_rollback,
}[STEP]()
