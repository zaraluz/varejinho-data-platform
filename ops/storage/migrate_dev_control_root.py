# Databricks notebook source
# ops/storage/migrate_dev_control_root.py
# Move o estado de controle de DEV de s3://varejinho-lake/_control/dev para o volume
# externo varejinho_dev.control.control_files (s3://varejinho-lake/_control_dev/).
#
# Por que dev sai de _control/: prod ganha um volume externo em _control/ e o Unity
# Catalog não aceita volume dentro de volume. Mover dev, e não prod, deixa o estado
# de produção onde está: em prod muda só o caminho de acesso.
#
# Um passo por execução (parâmetro `step`), nesta ordem:
#   plan    só leitura: o que existe na origem e no destino
#   copy    copia origem → destino e confere nome e tamanho de cada arquivo;
#           retomável: se o destino já é idêntico à origem, não copia de novo
#   verify  só leitura: toda a origem existe no destino com o mesmo tamanho; com o
#           destino ainda intocado, também a contagem de cada manifest Delta
#   delete  apaga a origem. Só depois de o pipeline de dev ter rodado verde lendo
#           do volume (runbook) e só se a conferência de arquivos passar de novo na hora
#
# Travas: dry_run=true por padrão; copy e delete exigem confirm_target=varejinho_dev;
# destino precisa ser um volume do catálogo de dev; origem precisa ser _control/dev.
# Roda como a operadora (dona do catálogo de dev e da external location).

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
SOURCE = param("source_root").rstrip("/")
TARGET = param("target_root").rstrip("/")

STEPS = ["plan", "copy", "verify", "delete"]
DEV_CATALOG = "varejinho_dev"
MANIFEST_DIR = "fact_partition_manifest"

if STEP not in STEPS:
    raise ValueError(f"step inválido: {STEP}. Opções: {STEPS}")
if SOURCE != "s3://varejinho-lake/_control/dev":
    raise ValueError(f"Origem inesperada: {SOURCE} (esperado s3://varejinho-lake/_control/dev)")
if not TARGET.startswith(f"/Volumes/{DEV_CATALOG}/"):
    raise ValueError(f"Destino precisa ser um volume de {DEV_CATALOG}: {TARGET}")
if not DRY_RUN and STEP in ("copy", "delete") and CONFIRM != DEV_CATALOG:
    raise ValueError(
        f"Escrita bloqueada: confirm_target='{CONFIRM}'. "
        f"Para executar de verdade, passe confirm_target={DEV_CATALOG}."
    )

PREFIX = "[dry-run] " if DRY_RUN else ""
print(f"step={STEP} dry_run={DRY_RUN}\norigem:  {SOURCE}\ndestino: {TARGET}\n")


# ── utilidades ─────────────────────────────────────────────────────────────

def _norm(path: str) -> str:
    # dbutils.fs.ls devolve caminhos de volume como dbfs:/Volumes/...; o S3 volta como s3://...
    return path[len("dbfs:"):] if path.startswith("dbfs:/Volumes/") else path


def list_files(root: str) -> dict[str, int]:
    """Arquivos sob root: caminho relativo → tamanho. Vazio se root não existe."""
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
            files[_norm(entry.path)[len(root) + 1:]] = entry.size
    return files


def top_level(files: dict[str, int]) -> dict[str, int]:
    resumo = {}
    for rel in files:
        chave = rel.split("/", 1)[0]
        resumo[chave] = resumo.get(chave, 0) + 1
    return dict(sorted(resumo.items()))


def missing_or_different(src: dict[str, int], dst: dict[str, int]) -> list[str]:
    """Arquivos da origem que faltam no destino ou têm outro tamanho."""
    return sorted(rel for rel, size in src.items() if dst.get(rel) != size)


def manifest_tables(files: dict[str, int]) -> list[str]:
    """Tabelas Delta de manifest: fact_partition_manifest/<entidade>/_delta_log/..."""
    return sorted({
        rel.split("/")[1] for rel in files
        if rel.startswith(f"{MANIFEST_DIR}/") and "/_delta_log/" in rel
    })


def check_no_active_runs() -> None:
    from databricks.sdk import WorkspaceClient

    active = [
        r.run_name for r in WorkspaceClient().jobs.list_runs(active_only=True)
        if "Migrate Dev Control Root" not in (r.run_name or "")
    ]
    if active:
        raise Exception(f"Há runs ativos; a cópia precisa de dev parado: {active}")
    print("✅ nenhum outro run ativo no workspace")


def check_files(src: dict[str, int], dst: dict[str, int]) -> None:
    diff = missing_or_different(src, dst)
    if diff:
        raise Exception(f"{len(diff)} arquivos da origem ausentes ou diferentes no destino: {diff[:10]}")
    extra = len(set(dst) - set(src))
    print(f"✅ {len(src)} arquivos da origem presentes no destino com o mesmo tamanho")
    if extra:
        print(f"ℹ️ {extra} arquivos só no destino (o pipeline já escreveu no volume)")


# ── passos ─────────────────────────────────────────────────────────────────

def step_plan() -> None:
    src, dst = list_files(SOURCE), list_files(TARGET)
    print(f"origem:  {len(src)} arquivos {top_level(src)}")
    print(f"destino: {len(dst)} arquivos {top_level(dst)}")
    print(f"manifests Delta na origem: {manifest_tables(src)}")
    if not src:
        print("⚠️ origem vazia: nada a migrar (já apagada?)")
    elif dst == src:
        print("ℹ️ destino já idêntico à origem: copy não faria nada")
    elif dst:
        print("⚠️ destino não vazio e diferente da origem: copy vai recusar")
    else:
        print("✅ pronto para copy")


def step_copy() -> None:
    src, dst = list_files(SOURCE), list_files(TARGET)
    if not src:
        raise Exception("Origem vazia: nada a copiar")
    if dst == src:
        print("ℹ️ destino já idêntico à origem; nada a copiar")
        return
    if dst:
        raise Exception(
            f"Destino não vazio ({len(dst)} arquivos) e diferente da origem. "
            "Confira antes; a cópia nunca mistura estados."
        )
    check_no_active_runs()
    # Uma cópia por entrada de topo: o destino (raiz do volume) já existe, e copiar
    # a pasta inteira para ele poderia criar um nível a mais (control_files/dev/...).
    for entry in dbutils.fs.ls(SOURCE):
        nome = entry.name.rstrip("/")
        print(f"{PREFIX}cp -r {SOURCE}/{nome} → {TARGET}/{nome}")
        if not DRY_RUN:
            dbutils.fs.cp(f"{SOURCE}/{nome}", f"{TARGET}/{nome}", recurse=entry.name.endswith("/"))
    if not DRY_RUN:
        check_files(src, list_files(TARGET))
        print("✅ copy concluído")


def step_verify() -> None:
    src, dst = list_files(SOURCE), list_files(TARGET)
    if not src:
        raise Exception("Origem vazia: não há o que conferir (já apagada?)")
    check_files(src, dst)
    if set(dst) != set(src):
        print("ℹ️ contagem dos manifests pulada: o destino já avançou além da cópia")
        return
    # Bytes iguais por arquivo já implicam a mesma tabela; a contagem prova que o
    # Delta abre e lê pelo caminho do volume, que é o que o pipeline vai fazer.
    for entidade in manifest_tables(src):
        n_src = spark.read.format("delta").load(f"{SOURCE}/{MANIFEST_DIR}/{entidade}").count()
        n_dst = spark.read.format("delta").load(f"{TARGET}/{MANIFEST_DIR}/{entidade}").count()
        if n_src != n_dst:
            raise Exception(f"{entidade}: origem {n_src} linhas, destino {n_dst}")
        print(f"✅ manifest {entidade}: {n_dst} linhas nos dois lados")
    print("✅ verify concluído")


def step_delete() -> None:
    src, dst = list_files(SOURCE), list_files(TARGET)
    if not src:
        print("ℹ️ origem já vazia; nada a apagar")
        return
    check_files(src, dst)   # de novo, na hora: nunca apagar sem a cópia presente
    check_no_active_runs()
    print(f"{PREFIX}rm -r {SOURCE} ({len(src)} arquivos)")
    if not DRY_RUN:
        dbutils.fs.rm(SOURCE, recurse=True)
        if list_files(SOURCE):
            raise Exception(f"Origem ainda tem arquivos depois do rm: {SOURCE}")
        print("✅ origem apagada; o estado de dev vive só no volume")


{
    "plan": step_plan,
    "copy": step_copy,
    "verify": step_verify,
    "delete": step_delete,
}[STEP]()
