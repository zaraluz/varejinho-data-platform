# Databricks notebook source
# validation/scd2/profile_scd2.py
# Gate B — diagnóstico read-only dos snapshots Bronze usados para reconstruir SCD2.
# Não escreve em Silver/Gold. O objetivo é entender a fonte antes de implementar história.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")

SCD2_CONFIG = {
    "produto": {
        "key": "id",
        "tracked_cols": [
            "descricaocompleta",
            "descricaoreduzida",
            "mercadologico1",
            "mercadologico2",
            "mercadologico3",
            "ncm1",
        ],
        "source_created_at": "datacadastro",
    },
    "fornecedor": {
        "key": "id",
        "tracked_cols": [
            "razaosocial",
            "nomefantasia",
            "cnpj",
            "id_situacaocadastro",
        ],
        "source_created_at": "datacadastro",
    },
    "mercadologico": {
        "key": "id",
        "tracked_cols": [
            "descricao",
            "mercadologico1",
            "mercadologico2",
            "mercadologico3",
            "nivel",
        ],
        "source_created_at": None,
    },
}


def fmt(v):
    return "NULL" if v is None else str(v)


blockers = []

print("\n=== GATE B — SCD2 SOURCE PROFILING ===")
print(f"Catálogo analisado: {CATALOG}")
print("Somente leitura: nenhuma tabela será alterada.\n")

for tabela, cfg in SCD2_CONFIG.items():
    full_name = f"{CATALOG}.bronze.{tabela}"
    print("\n" + "=" * 88)
    print(f"{tabela.upper()} | {full_name}")
    print("=" * 88)

    df = spark.table(full_name)
    cols = set(df.columns)
    key = cfg["key"]
    tracked = cfg["tracked_cols"]

    required = {key, "ingestion_date", *tracked}
    missing = sorted(required - cols)
    if missing:
        blockers.append(f"{tabela}: colunas ausentes: {missing}")
        print(f"❌ BLOQUEADOR — colunas ausentes: {missing}")
        continue

    ingestion_type = next(f.dataType.simpleString() for f in df.schema.fields if f.name == "ingestion_date")
    print(f"ingestion_date type: {ingestion_type}")
    print(f"natural key: {key}")
    print(f"tracked cols atuais: {tracked}")

    # Hash determinístico dos atributos que hoje definem uma versão.
    hash_expr = F.md5(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in tracked],
        )
    )
    dfx = df.withColumn("_hash", hash_expr)

    total_rows = dfx.count()
    distinct_ids = dfx.select(key).distinct().count()
    null_keys = dfx.filter(F.col(key).isNull()).count()
    snapshots_df = dfx.select("ingestion_date").distinct().orderBy("ingestion_date")
    snapshots = [r["ingestion_date"] for r in snapshots_df.collect()]

    print(f"rows totais: {total_rows:,}")
    print(f"ids distintos: {distinct_ids:,}")
    print(f"natural keys nulas: {null_keys:,}")
    print(f"snapshots distintos: {len(snapshots):,}")
    if snapshots:
        print(f"primeiro snapshot: {fmt(snapshots[0])}")
        print(f"último snapshot:   {fmt(snapshots[-1])}")

    # Volumetria dos snapshots mais recentes.
    snap_stats = (
        dfx.groupBy("ingestion_date")
        .agg(F.count("*").alias("rows"), F.countDistinct(key).alias("ids"))
        .orderBy(F.col("ingestion_date").desc())
        .limit(10)
        .collect()
    )
    print("\nÚltimos snapshots (até 10):")
    for r in snap_stats:
        print(f"  {fmt(r['ingestion_date'])}: rows={r['rows']:,} | ids={r['ids']:,}")

    # Duplicidade na grain esperada do snapshot: 1 linha por id por ingestion_date.
    dup_groups = (
        dfx.groupBy(key, "ingestion_date")
        .agg(F.count("*").alias("n"), F.countDistinct("_hash").alias("hashes"))
        .filter(F.col("n") > 1)
    )
    dup_group_count = dup_groups.count()
    conflicting_dup_count = dup_groups.filter(F.col("hashes") > 1).count()
    print("\nGrain do snapshot (id, ingestion_date):")
    print(f"  grupos duplicados: {dup_group_count:,}")
    print(f"  duplicados com conteúdo conflitante: {conflicting_dup_count:,}")

    # Para o diagnóstico histórico, escolhe deterministicamente um registro por id/snapshot.
    # Se houver conteúdo conflitante no mesmo snapshot, isso já foi explicitado acima.
    w_dedup = Window.partitionBy(key, "ingestion_date").orderBy(F.col("_hash").asc())
    dedup = dfx.withColumn("_rn", F.row_number().over(w_dedup)).filter("_rn = 1").drop("_rn")

    # Eventos de versão: primeira aparição ou mudança de hash em relação ao snapshot anterior.
    w_hist = Window.partitionBy(key).orderBy(F.col("ingestion_date").asc())
    hist = dedup.withColumn("_prev_hash", F.lag("_hash").over(w_hist))
    events = hist.filter(F.col("_prev_hash").isNull() | (F.col("_hash") != F.col("_prev_hash")))

    version_counts = events.groupBy(key).agg(F.count("*").alias("versions"))
    ids_changed = version_counts.filter("versions > 1").count()
    max_versions = version_counts.agg(F.max("versions")).collect()[0][0] or 0
    version_events = events.count()

    print("\nHistórico observável pelos snapshots:")
    print(f"  eventos de versão detectados: {version_events:,}")
    print(f"  ids com >1 versão: {ids_changed:,}")
    print(f"  máximo de versões observado em um id: {max_versions}")

    top_changed = version_counts.orderBy(F.col("versions").desc(), F.col(key).asc()).limit(5).collect()
    if top_changed:
        print("  top ids por nº de versões:")
        for r in top_changed:
            print(f"    id={r[key]} -> {r['versions']} versão(ões)")

    # Diferença entre os dois últimos snapshots: novos, ausentes e permanentes.
    if len(snapshots) >= 2:
        prev_snap = snapshots[-2]
        last_snap = snapshots[-1]
        prev_ids = dedup.filter(F.col("ingestion_date") == F.lit(prev_snap)).select(key).distinct()
        last_ids = dedup.filter(F.col("ingestion_date") == F.lit(last_snap)).select(key).distinct()

        new_ids = last_ids.join(prev_ids, key, "left_anti").count()
        missing_ids = prev_ids.join(last_ids, key, "left_anti").count()
        print("\nDelta entre os 2 últimos snapshots:")
        print(f"  novos ids no último snapshot: {new_ids:,}")
        print(f"  ids que sumiram no último snapshot: {missing_ids:,}")

    # Detecta gaps: entidade aparece, some em snapshot intermediário e reaparece depois.
    # Isso é importante porque ausência em full snapshot não deve virar 'delete' sem regra de negócio.
    snap_index = snapshots_df.withColumn(
        "_snap_idx", F.dense_rank().over(Window.orderBy("ingestion_date"))
    )
    presence = dedup.select(key, "ingestion_date").distinct().join(snap_index, "ingestion_date")
    gaps = (
        presence.groupBy(key)
        .agg(
            F.min("_snap_idx").alias("min_idx"),
            F.max("_snap_idx").alias("max_idx"),
            F.countDistinct("_snap_idx").alias("seen"),
        )
        .filter(F.col("seen") < (F.col("max_idx") - F.col("min_idx") + F.lit(1)))
        .count()
    )
    print(f"  ids com gap (sumiu e reapareceu): {gaps:,}")

    source_created_at = cfg.get("source_created_at")
    if source_created_at and source_created_at in cols:
        created_non_null = dfx.filter(F.col(source_created_at).isNotNull()).count()
        print("\nTimestamp de criação do ERP:")
        print(
            f"  {source_created_at} preenchido: {created_non_null:,}/{total_rows:,} "
            f"({(created_non_null / total_rows * 100) if total_rows else 0:.2f}%)"
        )
    elif source_created_at:
        print(f"\n⚠️ coluna de criação esperada não encontrada: {source_created_at}")

    if null_keys > 0:
        blockers.append(f"{tabela}: {null_keys} natural keys nulas")
    if conflicting_dup_count > 0:
        blockers.append(
            f"{tabela}: {conflicting_dup_count} grupos (id, ingestion_date) têm versões conflitantes"
        )

print("\n" + "=" * 88)
print("RESUMO DO PROFILING")
print("=" * 88)
if blockers:
    print("⚠️ Existem pontos que precisam de regra antes da implementação SCD2:")
    for b in blockers:
        print(f"  - {b}")
else:
    print("✅ Nenhum bloqueador estrutural detectado para começar a implementação SCD2.")

print(
    "\nNota semântica: para versões posteriores à primeira, ingestion_date representa "
    "'primeiro snapshot em que a mudança foi observada', não necessariamente o instante exato "
    "em que o ERP foi alterado."
)
