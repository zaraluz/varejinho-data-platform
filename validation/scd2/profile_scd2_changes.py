# Databricks notebook source
# validation/scd2/profile_scd2_changes.py
# Gate B2 — atribui mudanças observadas de produto aos atributos que mudaram.
# Somente leitura: não altera Bronze, Silver ou Gold.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
TABLE = f"{CATALOG}.bronze.produto"
KEY = "id"
SNAPSHOT = "ingestion_date"

# Colunas que HOJE definem hash_versao. Ainda são hipótese de modelagem,
# não decisão final. O objetivo deste profiler é justamente testar essa hipótese.
CURRENT_TYPE2_CANDIDATES = [
    "descricaocompleta",
    "descricaoreduzida",
    "mercadologico1",
    "mercadologico2",
    "mercadologico3",
    "ncm1",
]

# Outros atributos publicados na dimensão Gold e que também merecem decisão
# explícita Type 1 x Type 2. Só entram se existirem na Bronze.
EXTRA_DIMENSION_CANDIDATES = [
    "id_tipoembalagem",
    "id_tipomercadoria",
    "pesoliquido",
    "pesobruto",
    "id_situacaocadastro",
]


def changed(col_name: str):
    """Mudança null-safe entre snapshot atual e anterior."""
    return ~F.col(col_name).eqNullSafe(F.col(f"_prev_{col_name}"))


print("\n=== GATE B2 — SCD2 CHANGE ATTRIBUTION | PRODUTO ===")
print(f"Fonte: {TABLE}")
print("Somente leitura: nenhuma tabela será alterada.\n")

raw = spark.table(TABLE)
cols = set(raw.columns)

required = {KEY, SNAPSHOT, *CURRENT_TYPE2_CANDIDATES}
missing_required = sorted(required - cols)
if missing_required:
    raise Exception(f"Colunas obrigatórias ausentes: {missing_required}")

candidate_cols = CURRENT_TYPE2_CANDIDATES + [
    c for c in EXTRA_DIMENSION_CANDIDATES if c in cols
]
missing_optional = [c for c in EXTRA_DIMENSION_CANDIDATES if c not in cols]

print(f"Atributos do hash atual: {CURRENT_TYPE2_CANDIDATES}")
print(f"Outros candidatos presentes: {[c for c in candidate_cols if c not in CURRENT_TYPE2_CANDIDATES]}")
if missing_optional:
    print(f"Candidatos opcionais ausentes na Bronze: {missing_optional}")

# Hash atual: fingerprint apenas dos atributos que hoje são tratados como Type 2.
hash_expr = F.md5(
    F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
            for c in CURRENT_TYPE2_CANDIDATES
        ],
    )
)

base = raw.withColumn("_hash", hash_expr)

# O profiling anterior provou grain única por (id, ingestion_date), mas mantemos
# dedup determinístico como proteção para que este notebook seja repetível.
w_dedup = Window.partitionBy(KEY, SNAPSHOT).orderBy(F.col("_hash").asc())
dedup = (
    base.withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# Compara cada produto apenas contra o snapshot anterior do MESMO produto.
w_hist = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
hist = dedup
hist = hist.withColumn("_prev_snapshot", F.lag(SNAPSHOT).over(w_hist))
hist = hist.withColumn("_prev_hash", F.lag("_hash").over(w_hist))
for c in candidate_cols:
    hist = hist.withColumn(f"_prev_{c}", F.lag(c).over(w_hist))

# Primeira aparição não é uma "mudança"; é a versão inicial.
comparisons = hist.filter(F.col("_prev_snapshot").isNotNull())

# Os 39 eventos encontrados no profiling anterior devem aparecer aqui como
# mudanças do hash atual (se a Bronze não mudou entre as execuções).
hash_changes = comparisons.filter(F.col("_hash") != F.col("_prev_hash"))
hash_change_count = hash_changes.count()
changed_ids = hash_changes.select(KEY).distinct().count()

print("\n--- A. EVENTOS DO HASH ATUAL ---")
print(f"eventos em que hash atual mudou: {hash_change_count:,}")
print(f"ids afetados: {changed_ids:,}")

# Atribuição: em quantos dos eventos de hash cada atributo mudou?
print("\nAtribuição dos eventos do hash atual por atributo:")
attribute_stats = []
for c in CURRENT_TYPE2_CANDIDATES:
    n_events = hash_changes.filter(changed(c)).count()
    n_ids = hash_changes.filter(changed(c)).select(KEY).distinct().count()
    pct = (n_events / hash_change_count * 100) if hash_change_count else 0
    attribute_stats.append((c, n_events, n_ids, pct))

for c, n_events, n_ids, pct in sorted(attribute_stats, key=lambda x: (-x[1], x[0])):
    print(f"  {c:<24} eventos={n_events:>4} | ids={n_ids:>4} | {pct:6.2f}% dos eventos")

# Assinatura da mudança: mostra quais atributos mudaram juntos no mesmo evento.
signature_expr = F.concat_ws(
    " + ",
    *[F.when(changed(c), F.lit(c)) for c in CURRENT_TYPE2_CANDIDATES],
)

signatures = (
    hash_changes.withColumn("_change_signature", signature_expr)
    .groupBy("_change_signature")
    .agg(F.count("*").alias("events"), F.countDistinct(KEY).alias("ids"))
    .orderBy(F.col("events").desc(), F.col("_change_signature").asc())
)

print("\nCombinações de atributos que mudaram juntas:")
for r in signatures.collect():
    print(f"  {r['_change_signature']}: {r['events']} evento(s) | {r['ids']} id(s)")

# Exemplos humanos dos eventos reais — antes/depois apenas dos campos que mudaram.
print("\nExemplos reais de mudanças do hash atual (até 15 eventos):")
example_rows = (
    hash_changes.orderBy(F.col(SNAPSHOT).asc(), F.col(KEY).asc())
    .limit(15)
    .collect()
)
for r in example_rows:
    print(f"\n  id={r[KEY]} | {r['_prev_snapshot']} -> {r[SNAPSHOT]}")
    for c in CURRENT_TYPE2_CANDIDATES:
        before = r[f"_prev_{c}"]
        after = r[c]
        if before != after:
            print(f"    {c}: {repr(before)} -> {repr(after)}")

# ---------------------------------------------------------------------------
# B. CAMPOS FORA DO HASH ATUAL
# ---------------------------------------------------------------------------
# Isto responde uma pergunta diferente: existem atributos relevantes da dimensão
# mudando sem gerar nova versão hoje? Se sim, precisamos decidir Type 1 x Type 2.
print("\n--- B. TODOS OS CANDIDATOS DA DIMENSÃO ---")
print("Mudanças observadas em qualquer comparação consecutiva de snapshots:")

candidate_change_stats = []
for c in candidate_cols:
    n_events = comparisons.filter(changed(c)).count()
    n_ids = comparisons.filter(changed(c)).select(KEY).distinct().count()
    candidate_change_stats.append((c, n_events, n_ids, c in CURRENT_TYPE2_CANDIDATES))

for c, n_events, n_ids, in_hash in sorted(candidate_change_stats, key=lambda x: (-x[1], x[0])):
    role = "DENTRO DO HASH" if in_hash else "FORA DO HASH"
    print(f"  {c:<24} eventos={n_events:>4} | ids={n_ids:>4} | {role}")

outside_hash = [c for c in candidate_cols if c not in CURRENT_TYPE2_CANDIDATES]
outside_change_events = 0
if outside_hash:
    any_outside_change = None
    for c in outside_hash:
        expr = changed(c)
        any_outside_change = expr if any_outside_change is None else (any_outside_change | expr)
    outside_change_events = comparisons.filter(any_outside_change).count()
    print(f"\nEventos com pelo menos um candidato FORA do hash alterado: {outside_change_events:,}")

# ---------------------------------------------------------------------------
# C. DATAALTERACAO DO ERP x PRIMEIRO SNAPSHOT ONDE OBSERVAMOS A MUDANÇA
# ---------------------------------------------------------------------------
print("\n--- C. DATAALTERACAO x INGESTION_DATE ---")
if "dataalteracao" not in cols:
    print("⚠️ dataalteracao não existe na Bronze de produto; não é possível comparar timestamps.")
else:
    parsed = F.coalesce(
        F.to_timestamp(F.col("dataalteracao"), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col("dataalteracao").cast("timestamp"),
    )

    ts_events = hash_changes.withColumn("_source_altered_at", parsed)
    ts_events = ts_events.withColumn(
        "_delay_days",
        F.datediff(F.col(SNAPSHOT), F.to_date(F.col("_source_altered_at"))),
    )

    ts_summary = ts_events.agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("_source_altered_at").isNotNull(), 1).otherwise(0)).alias("parsed"),
        F.sum(F.when(F.col("_delay_days") == 0, 1).otherwise(0)).alias("same_day"),
        F.sum(F.when(F.col("_delay_days") > 0, 1).otherwise(0)).alias("before_observed"),
        F.sum(F.when(F.col("_delay_days") < 0, 1).otherwise(0)).alias("after_observed"),
        F.expr("percentile_approx(_delay_days, 0.5)").alias("median_delay_days"),
        F.max("_delay_days").alias("max_delay_days"),
    ).collect()[0]

    print(f"eventos de mudança analisados: {ts_summary['events']:,}")
    print(f"dataalteracao parseável:       {ts_summary['parsed']:,}")
    print(f"mesmo dia da ingestão:         {ts_summary['same_day']:,}")
    print(f"ERP anterior à observação:     {ts_summary['before_observed']:,}")
    print(f"ERP posterior à observação:    {ts_summary['after_observed']:,}")
    print(f"mediana atraso observação:     {ts_summary['median_delay_days']} dia(s)")
    print(f"maior atraso observado:        {ts_summary['max_delay_days']} dia(s)")

    print("\nAmostra dataalteracao x primeiro snapshot que capturou a mudança:")
    for r in (
        ts_events.select(KEY, "_prev_snapshot", SNAPSHOT, "dataalteracao", "_source_altered_at", "_delay_days")
        .orderBy(F.col(SNAPSHOT).asc(), F.col(KEY).asc())
        .limit(15)
        .collect()
    ):
        print(
            f"  id={r[KEY]} | observado={r[SNAPSHOT]} | "
            f"dataalteracao={r['dataalteracao']} | delay={r['_delay_days']} dia(s)"
        )

print("\n=== LEITURA PARA MODELING ===")
print("1) A seção A explica os 39 eventos do hash atual: quais atributos causaram cada nova fingerprint.")
print("2) A seção B procura mudanças em atributos relevantes que hoje NÃO participam do hash.")
print("3) A seção C testa se dataalteracao pode ser melhor valid_from que ingestion_date para versões futuras.")
print("4) Este notebook não decide Type 1/Type 2 sozinho; ele fornece evidência para a decisão de negócio/modelagem.")
