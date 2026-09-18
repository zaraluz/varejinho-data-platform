# Databricks notebook source
# pipeline/silver/profile_scd2_supplier_changes.py
# Gate B7B — change attribution real de fornecedor.
# Mostra somente atributos que realmente mudaram e exemplos before/after.
# Somente leitura.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
TABLE = f"{CATALOG}.bronze.fornecedor"
KEY = "id"
SNAPSHOT = "ingestion_date"

CURRENT_HASH = [
    "razaosocial",
    "nomefantasia",
    "cnpj",
    "id_situacaocadastro",
]
TEMPORAL = {"datacadastro", "dataalteracao"}

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B7B só pode executar em *_dev. Recebido: {CATALOG}")

df = spark.table(TABLE)
cols = df.columns
colset = set(cols)

excluded = {KEY, SNAPSHOT, "_metadata", *TEMPORAL}
business_cols = [c for c in cols if c not in excluded]

w = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())

hist = df
for c in business_cols:
    hist = hist.withColumn(f"_prev__{c}", F.lag(F.col(c)).over(w))
hist = hist.withColumn("_prev_snapshot", F.lag(F.col(SNAPSHOT)).over(w))

change_exprs = {}
for c in business_cols:
    prev = f"_prev__{c}"
    change_exprs[c] = (
        F.col("_prev_snapshot").isNotNull()
        & (~F.col(c).eqNullSafe(F.col(prev)))
    )

changed_stats = []
for c, expr in change_exprs.items():
    events = hist.filter(expr).count()
    if events:
        ids = hist.filter(expr).select(KEY).distinct().count()
        changed_stats.append((c, events, ids))

changed_stats.sort(key=lambda x: (-x[1], x[0]))
changed_cols = [c for c, _, _ in changed_stats]

if not changed_cols:
    print("\n=== GATE B7B — SUPPLIER CHANGE ATTRIBUTION ===")
    print("Nenhum atributo de negócio mudou entre snapshots consecutivos.")
    dbutils.notebook.exit("NO_CHANGES")

any_change = None
for c in changed_cols:
    any_change = change_exprs[c] if any_change is None else (any_change | change_exprs[c])

events = hist.filter(any_change)

# Materializa flags para descobrir combinações de atributos que mudaram juntos.
flagged = events
flag_names = []
for c in changed_cols:
    flag = f"_chg__{c}"
    flag_names.append(flag)
    flagged = flagged.withColumn(flag, change_exprs[c])

changed_array = F.array(*[
    F.when(F.col(flag), F.lit(c)).otherwise(F.lit(None).cast("string"))
    for c, flag in zip(changed_cols, flag_names)
])
flagged = flagged.withColumn("_tmp", changed_array)
flagged = flagged.withColumn(
    "_changed_cols", F.expr("filter(_tmp, x -> x is not null)")
).drop("_tmp")
flagged = flagged.withColumn("_combo", F.concat_ws(" + ", F.col("_changed_cols")))

print("\n=== GATE B7B — SUPPLIER CHANGE ATTRIBUTION ===")
print(f"Fonte: {TABLE}")
print("Somente leitura: nenhuma tabela será alterada.\n")

print("--- A. ATRIBUTOS QUE REALMENTE MUDARAM ---")
for c, n_events, n_ids in changed_stats:
    tag = "DENTRO DO HASH ATUAL" if c in CURRENT_HASH else "FORA DO HASH ATUAL"
    print(f"  {c:<32} eventos={n_events:>4,} | ids={n_ids:>4,} | {tag}")

print(f"\neventos totais com mudança de negócio: {events.count():,}")
print(f"ids afetados:                           {events.select(KEY).distinct().count():,}")

print("\n--- B. COMBINAÇÕES DE CAMPOS QUE MUDARAM JUNTOS ---")
for r in (
    flagged.groupBy("_combo")
    .agg(F.count("*").alias("events"), F.countDistinct(KEY).alias("ids"))
    .orderBy(F.col("events").desc(), F.col("_combo").asc())
    .collect()
):
    print(f"  {r['_combo']}: {r['events']} evento(s) | {r['ids']} id(s)")

print("\n--- C. EXEMPLOS REAIS BEFORE / AFTER ---")
sample_rows = flagged.orderBy(F.col(SNAPSHOT).asc(), F.col(KEY).asc()).limit(30).collect()
for r in sample_rows:
    changed = r["_changed_cols"]
    print(f"\n  id={r[KEY]} | {r['_prev_snapshot']} -> {r[SNAPSHOT]}")
    for c in changed:
        print(f"    {c}: {repr(r[f'_prev__{c}'])} -> {repr(r[c])}")

print("\n--- D. HASH ATUAL x MUDANÇAS FORA DO HASH ---")
hash_change = None
for c in CURRENT_HASH:
    expr = change_exprs[c]
    hash_change = expr if hash_change is None else (hash_change | expr)

outside_cols = [c for c in changed_cols if c not in CURRENT_HASH]
outside_change = None
for c in outside_cols:
    expr = change_exprs[c]
    outside_change = expr if outside_change is None else (outside_change | expr)

hash_events = hist.filter(hash_change).count() if hash_change is not None else 0
outside_events = hist.filter(outside_change).count() if outside_change is not None else 0

print(f"eventos explicados pelo hash atual: {hash_events:,}")
print(f"eventos com mudança fora do hash:   {outside_events:,}")
print("Mudança fora do hash NÃO significa automaticamente Type 2; exige semântica de negócio.")

print("\n--- E. POLÍTICA TEMPORAL POSSÍVEL ---")
print("datacadastro existe:   " + ("SIM" if "datacadastro" in colset else "NÃO"))
print("dataalteracao existe:  " + ("SIM" if "dataalteracao" in colset else "NÃO"))
if "dataalteracao" not in colset:
    print("Para versões posteriores à primeira, o melhor boundary disponível hoje é ingestion_date:")
    print("  = primeiro snapshot em que a mudança foi observada, não instante exato no ERP.")

print("\n=== LEITURA PARA MODELING ===")
print("1) Não colocar todos os campos mutáveis no hash.")
print("2) Type 2 só para atributo cujo valor histórico precisa acompanhar fatos passados.")
print("3) Type 1 para contato, cadastro operacional/configuração atual e correções sem valor histórico.")
print("4) Se os atributos escolhidos como Type 2 não mudaram na janela real, validar criação de versão com fixture sintética isolada.")
