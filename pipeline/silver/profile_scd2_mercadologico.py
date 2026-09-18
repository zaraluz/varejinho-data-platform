# Databricks notebook source
# pipeline/silver/profile_scd2_mercadologico.py
# Gate B8A — profiling semântico read-only de mercadologico antes do modeling SCD2.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
TABLE = f"{CATALOG}.bronze.mercadologico"
KEY = "id"
SNAPSHOT = "ingestion_date"

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B8A só pode executar em *_dev. Recebido: {CATALOG}")

df = spark.table(TABLE)
cols = df.columns
colset = set(cols)

print("\n=== GATE B8A — MERCADOLOGICO SCD2 SEMANTIC PROFILING ===")
print(f"Fonte: {TABLE}")
print("Somente leitura: nenhuma tabela será alterada.\n")

print("--- A. SCHEMA DA BRONZE ---")
for field in df.schema.fields:
    print(f"  {field.name:<32} {field.dataType.simpleString()}")

required = {KEY, SNAPSHOT}
missing = sorted(required - colset)
if missing:
    raise Exception(f"Colunas obrigatórias ausentes: {missing}")

snapshots = [
    r[SNAPSHOT]
    for r in df.select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()
]
latest = snapshots[-1]
latest_df = df.filter(F.col(SNAPSHOT) == F.lit(latest))

print("\n--- B. CONTEXTO DOS SNAPSHOTS ---")
print(f"snapshots:        {len(snapshots):,}")
print(f"primeiro:         {snapshots[0]}")
print(f"último:           {latest}")
print(f"rows último:      {latest_df.count():,}")
print(f"ids último:       {latest_df.select(KEY).distinct().count():,}")
print(f"natural keys nulas: {df.filter(F.col(KEY).isNull()).count():,}")

dup_groups = (
    df.groupBy(KEY, SNAPSHOT).count().filter(F.col("count") > 1).count()
)
print(f"duplicatas por (id, ingestion_date): {dup_groups:,}")

# Campos auxiliares/temporais não entram automaticamente como atributos de negócio.
name_l = {c: c.lower() for c in cols}
temporal_candidates = [
    c for c in cols
    if c != SNAPSHOT and any(token in c.lower() for token in [
        "data", "date", "dt", "cadastro", "alter", "criacao", "created", "updated"
    ])
]
metadata_cols = {SNAPSHOT, "_metadata", *temporal_candidates}
business_cols = [c for c in cols if c not in {KEY, *metadata_cols}]

print("\n--- C. PERFIL DOS ATRIBUTOS NO ÚLTIMO SNAPSHOT ---")
print("coluna | nulls | distintos | amostra se domínio pequeno")
for c in business_cols:
    nulls = latest_df.filter(F.col(c).isNull()).count()
    distincts = latest_df.select(c).distinct().count()
    print(f"  {c:<32} nulls={nulls:>6,} | distincts={distincts:>6,}", end="")
    if distincts <= 20:
        vals = [r[c] for r in latest_df.select(c).distinct().orderBy(c).limit(25).collect()]
        print(f" | valores={vals}")
    else:
        print()

# Change attribution null-safe: a existência de linha anterior é detectada por prev_snapshot,
# não pela nulidade do próprio atributo.
w = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
hist = df.withColumn("_prev_snapshot", F.lag(F.col(SNAPSHOT)).over(w))
for c in business_cols:
    hist = hist.withColumn(f"_prev__{c}", F.lag(F.col(c)).over(w))

changed_stats = []
change_exprs = {}
for c in business_cols:
    expr = (
        F.col("_prev_snapshot").isNotNull()
        & (~F.col(c).eqNullSafe(F.col(f"_prev__{c}")))
    )
    change_exprs[c] = expr
    events = hist.filter(expr).count()
    if events:
        ids = hist.filter(expr).select(KEY).distinct().count()
        changed_stats.append((c, events, ids))

changed_stats.sort(key=lambda x: (-x[1], x[0]))

print("\n--- D. CHANGE ATTRIBUTION POR ATRIBUTO ---")
if changed_stats:
    for c, events, ids in changed_stats:
        print(f"  {c:<32} eventos={events:>5,} | ids={ids:>5,}")
else:
    print("  Nenhuma mudança de atributo de negócio observada entre snapshots consecutivos.")

if changed_stats:
    any_change = None
    for c, _, _ in changed_stats:
        any_change = change_exprs[c] if any_change is None else (any_change | change_exprs[c])
    events_df = hist.filter(any_change)
    print(f"\neventos com qualquer mudança: {events_df.count():,}")
    print(f"ids com qualquer mudança:     {events_df.select(KEY).distinct().count():,}")

    print("\nExemplos before/after (até 20 eventos):")
    for r in events_df.orderBy(SNAPSHOT, KEY).limit(20).collect():
        changed = [
            c for c, _, _ in changed_stats
            if r["_prev_snapshot"] is not None and r[c] != r[f"_prev__{c}"]
        ]
        print(f"\n  id={r[KEY]} | {r['_prev_snapshot']} -> {r[SNAPSHOT]}")
        for c in changed:
            print(f"    {c}: {repr(r[f'_prev__{c}'])} -> {repr(r[c])}")

print("\n--- E. CANDIDATOS TEMPORAIS DA FONTE ---")
if not temporal_candidates:
    print("  Nenhuma coluna temporal candidata encontrada pelo schema.")
else:
    for c in temporal_candidates:
        non_null = df.filter(F.col(c).isNotNull()).count()
        samples = [r[c] for r in df.select(c).filter(F.col(c).isNotNull()).distinct().limit(8).collect()]
        print(f"  {c}: non_null={non_null:,}/{df.count():,} | amostras={samples}")

print("\n--- F. ESTRUTURA HIERÁRQUICA ---")
if "nivel" in colset:
    print("Distribuição por nivel no último snapshot:")
    for r in latest_df.groupBy("nivel").count().orderBy("nivel").collect():
        print(f"  nivel={r['nivel']} -> {r['count']:,}")

hier_cols = [c for c in ["mercadologico1","mercadologico2","mercadologico3","mercadologico4","mercadologico5"] if c in colset]
if hier_cols:
    print(f"colunas hierárquicas encontradas: {hier_cols}")
    print("Amostra de caminhos atuais:")
    for r in latest_df.select(KEY, *hier_cols, *(["nivel"] if "nivel" in colset else []), *(["descricao"] if "descricao" in colset else [])).orderBy(KEY).limit(20).collect():
        print("  " + " | ".join(f"{c}={repr(r[c])}" for c in [KEY, *hier_cols, *(["nivel"] if "nivel" in colset else []), *(["descricao"] if "descricao" in colset else [])]))

print("\n--- G. MODELING CHECKPOINT ---")
print("O profiler NÃO decide Type 1/Type 2.")
print("Precisamos fechar:")
print("  1) quais campos representam identidade/classificação histórica do nó mercadológico;")
print("  2) quais campos são apenas rótulos/correções atuais;")
print("  3) qual boundary usar na primeira versão, já que o modelo antigo inventava 2020-01-01;")
print("  4) como tratar versões posteriores se a fonte não possui timestamp de alteração;")
print("  5) fixture sintética obrigatória se a janela real continuar sem mudanças.")
