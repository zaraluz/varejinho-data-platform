# Databricks notebook source
# pipeline/silver/profile_scd2_supplier.py
# Gate B7A — profiling semântico de fornecedor antes de modelar Type 1/Type 2.
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

CURRENT_TRACKED = [
    "razaosocial",
    "nomefantasia",
    "cnpj",
    "id_situacaocadastro",
]

TEMPORAL_CANDIDATES = ["datacadastro", "dataalteracao"]

if not CATALOG.endswith("_dev"):
    raise Exception(f"Gate B7A só pode executar em *_dev. Recebido: {CATALOG}")

df = spark.table(TABLE)
cols = df.columns
colset = set(cols)

required = {KEY, SNAPSHOT, *CURRENT_TRACKED}
missing = sorted(required - colset)
if missing:
    raise Exception(f"Colunas obrigatórias ausentes em {TABLE}: {missing}")

print("\n=== GATE B7A — SUPPLIER SCD2 SEMANTIC PROFILING ===")
print(f"Fonte: {TABLE}")
print("Somente leitura: nenhuma tabela será alterada.\n")

print("--- A. SCHEMA DA BRONZE ---")
for f in df.schema.fields:
    print(f"  {f.name:<32} {f.dataType.simpleString()}")

snapshots = [r[SNAPSHOT] for r in df.select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()]
latest = snapshots[-1]
latest_df = df.filter(F.col(SNAPSHOT) == F.lit(latest))

print("\n--- B. CONTEXTO DOS SNAPSHOTS ---")
print(f"snapshots:        {len(snapshots)}")
print(f"primeiro:         {snapshots[0]}")
print(f"último:           {latest}")
print(f"rows último:      {latest_df.count():,}")
print(f"ids último:       {latest_df.select(KEY).distinct().count():,}")

# Colunas técnicas/temporais ficam fora do ranking de atributos de negócio.
excluded = {KEY, SNAPSHOT, "_metadata", *TEMPORAL_CANDIDATES}
business_cols = [c for c in cols if c not in excluded]

print("\n--- C. PERFIL DOS ATRIBUTOS NO ÚLTIMO SNAPSHOT ---")
print("coluna | nulls | distintos | leitura")
for c in business_cols:
    stats = latest_df.agg(
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias("nulls"),
        F.countDistinct(F.col(c)).alias("distincts"),
    ).collect()[0]
    nulls = int(stats["nulls"] or 0)
    distincts = int(stats["distincts"] or 0)
    leitura = "tracked atual" if c in CURRENT_TRACKED else "fora do hash atual"
    print(f"  {c:<32} nulls={nulls:>6,} | distincts={distincts:>6,} | {leitura}")

    # Para domínios pequenos, mostra os valores para entendermos a semântica.
    if 0 < distincts <= 20:
        values = [r[c] for r in latest_df.select(c).distinct().orderBy(c).collect()]
        print(f"    valores: {values}")

# Mudanças por coluna entre snapshots consecutivos.
w = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
hist = df
for c in business_cols:
    hist = hist.withColumn(f"_prev__{c}", F.lag(F.col(c)).over(w))

print("\n--- D. CHANGE ATTRIBUTION POR ATRIBUTO ---")
print("Mudanças observadas entre snapshots consecutivos:")
total_change_any = None
for c in business_cols:
    prev = f"_prev__{c}"
    changed = F.col(prev).isNotNull() & (~F.col(c).eqNullSafe(F.col(prev)))
    events = hist.filter(changed).count()
    ids = hist.filter(changed).select(KEY).distinct().count()
    tag = "NO HASH ATUAL" if c in CURRENT_TRACKED else "FORA DO HASH ATUAL"
    print(f"  {c:<32} eventos={events:>5,} | ids={ids:>5,} | {tag}")
    total_change_any = changed if total_change_any is None else (total_change_any | changed)

any_events = hist.filter(total_change_any).count() if total_change_any is not None else 0
any_ids = hist.filter(total_change_any).select(KEY).distinct().count() if total_change_any is not None else 0
print(f"\neventos com qualquer atributo de negócio alterado: {any_events:,}")
print(f"ids com qualquer atributo de negócio alterado:     {any_ids:,}")

print("\n--- E. COLUNAS TEMPORAIS DO ERP ---")
for c in TEMPORAL_CANDIDATES:
    if c not in colset:
        print(f"  {c}: AUSENTE")
        continue

    parsed = F.coalesce(
        F.to_timestamp(F.col(c), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col(c).cast("timestamp"),
    )
    temp = df.withColumn("_parsed", parsed)
    non_null = temp.filter(F.col(c).isNotNull()).count()
    parseable = temp.filter(F.col("_parsed").isNotNull()).count()
    after_snapshot = temp.filter(
        F.col("_parsed").isNotNull()
        & (F.to_date(F.col("_parsed")) > F.col(SNAPSHOT).cast("date"))
    ).count()
    min_ts, max_ts = temp.agg(F.min("_parsed"), F.max("_parsed")).collect()[0]
    print(
        f"  {c}: non_null={non_null:,}/{df.count():,} | parseable={parseable:,} | "
        f"posterior_ao_snapshot={after_snapshot:,} | min={min_ts} | max={max_ts}"
    )

# Verifica se dataalteracao se move mesmo quando os atributos de negócio não mudam.
if "dataalteracao" in colset:
    parsed_alt = F.coalesce(
        F.to_timestamp(F.col("dataalteracao"), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col("dataalteracao").cast("timestamp"),
    )
    h_alt = (
        df.withColumn("_dataalteracao_ts", parsed_alt)
          .withColumn("_prev_dataalteracao_ts", F.lag("_dataalteracao_ts").over(w))
    )
    alt_changed = h_alt.filter(
        F.col("_prev_dataalteracao_ts").isNotNull()
        & (~F.col("_dataalteracao_ts").eqNullSafe(F.col("_prev_dataalteracao_ts")))
    )
    print(
        f"  dataalteracao mudou entre snapshots: eventos={alt_changed.count():,} | "
        f"ids={alt_changed.select(KEY).distinct().count():,}"
    )

print("\n--- F. MODELING CHECKPOINT ---")
print("O profiler NÃO decide Type 1/Type 2.")
print("Próximas decisões:")
print("  1) quais atributos alteram a identidade histórica do fornecedor;")
print("  2) quais são apenas correções/campos de exibição e devem ser Type 1;")
print("  3) se id_situacaocadastro deve ser historizado e exposto na Gold;")
print("  4) se dataalteracao existe e é confiável para versões posteriores;")
print("  5) como validar SCD2 sem mudanças reais: fixture sintética isolada.")
