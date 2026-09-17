# Databricks notebook source
# pipeline/silver/incremental_scd2_product.py
# Gate B5 — manutenção incremental e idempotente do SCD2 de produto.
#
# Princípios:
# - watermark do SCD2 é independente do watermark de extração do Pentaho;
# - compara snapshots Bronze consecutivos, não depende apenas do estado current da Silver;
# - Type 2 fecha a versão anterior e insere nova versão;
# - Type 1 atualiza todas as versões sem criar nova versão;
# - ausência no snapshot NÃO é tratada como deleção;
# - o watermark committed só é avançado em task posterior, depois do Quality Gate.

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE = f"{CATALOG}.bronze.produto"
SILVER = f"{CATALOG}.silver.produto"
CONTROL_SCHEMA = f"{CATALOG}.control"
CONTROL_TABLE = f"{CONTROL_SCHEMA}.scd2_watermark"
ENTITY = "produto"
KEY = "id"
SNAPSHOT = "ingestion_date"

TYPE2_COLS = [
    "descricaocompleta",
    "mercadologico1",
    "mercadologico2",
    "mercadologico3",
    "ncm1",
    "id_tipoembalagem",
]

TYPE1_COLS = [
    "descricaoreduzida",
    "id_tipomercadoria",  # provisório até a semântica do ERP ser confirmada
    "pesoliquido",
    "pesobruto",
]


def parse_erp_timestamp(col_name: str):
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"),
        F.col(col_name).cast("timestamp"),
    )


def type2_hash():
    return F.md5(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in TYPE2_COLS],
        )
    )


def changed_expr(left_alias: str, right_alias: str, cols):
    expr = None
    for c in cols:
        current = ~F.col(f"{left_alias}.{c}").eqNullSafe(F.col(f"{right_alias}.{c}"))
        expr = current if expr is None else (expr | current)
    return expr


def ensure_dev():
    if not CATALOG.endswith("_dev"):
        raise Exception(
            f"Proteção de hardening: incremental_scd2_product só pode executar em catálogo *_dev. Recebido: {CATALOG}"
        )


def ensure_control_table():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            entity STRING,
            last_processed_snapshot DATE,
            candidate_snapshot DATE,
            status STRING,
            updated_at TIMESTAMP
        ) USING DELTA
    """)


def get_watermark_row():
    rows = spark.table(CONTROL_TABLE).filter(F.col("entity") == ENTITY).collect()
    if len(rows) > 1:
        raise Exception(f"Controle inválido: mais de uma linha de watermark para entity={ENTITY}")
    return rows[0] if rows else None


def baseline_is_equivalent(bronze, silver) -> tuple[bool, str]:
    """Prova mínima para semear o watermark após um backfill já validado.

    O watermark não pode ser inferido de MAX(scd_source_snapshot), pois pode haver
    snapshots processados sem qualquer mudança Type 2. Por isso, quando o controle
    ainda não existe, comparamos a Silver atual contra TODO o histórico Bronze.
    """
    w_hist = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).asc())
    expected = (
        bronze.withColumn("_expected_hash", type2_hash())
              .withColumn("_prev_hash", F.lag("_expected_hash").over(w_hist))
              .filter(F.col("_prev_hash").isNull() | (F.col("_expected_hash") != F.col("_prev_hash")))
    )

    expected_rows = expected.count()
    actual_rows = silver.count()
    if expected_rows != actual_rows:
        return False, f"cardinalidade divergente: expected={expected_rows:,}, actual={actual_rows:,}"

    current = silver.filter(F.col("is_current"))
    bad_current = (
        current.groupBy(KEY).count().filter(F.col("count") != 1).count()
        + abs(current.select(KEY).distinct().count() - bronze.select(KEY).distinct().count())
    )
    if bad_current:
        return False, f"estado current inconsistente: {bad_current} problema(s)"

    latest_date = bronze.agg(F.max(SNAPSHOT)).collect()[0][0]
    latest = (
        bronze.filter(F.col(SNAPSHOT) == F.lit(latest_date))
              .withColumn("_latest_hash", type2_hash())
              .select(KEY, "_latest_hash", *[F.col(c).alias(f"_latest_{c}") for c in TYPE1_COLS])
    )

    current_hash_mismatch = (
        current.select(KEY, "hash_versao")
               .join(latest.select(KEY, "_latest_hash"), on=KEY, how="full")
               .filter(~F.col("hash_versao").eqNullSafe(F.col("_latest_hash")))
               .count()
    )
    if current_hash_mismatch:
        return False, f"current Type 2 diverge do último snapshot em {current_hash_mismatch:,} id(s)"

    type1_join = silver.join(latest, on=KEY, how="left")
    for c in TYPE1_COLS:
        mismatch = type1_join.filter(~F.col(c).eqNullSafe(F.col(f"_latest_{c}"))).count()
        if mismatch:
            return False, f"Type 1 {c} diverge do último snapshot em {mismatch:,} linha(s)"

    return True, f"Silver equivalente ao histórico Bronze até {latest_date}"


def seed_watermark_if_needed(bronze, silver):
    row = get_watermark_row()
    if row:
        return row

    ok, detail = baseline_is_equivalent(bronze, silver)
    if not ok:
        raise Exception(
            "Watermark ainda não existe e a Silver não é equivalente ao backfill completo. "
            f"Não é seguro inicializar o controle: {detail}"
        )

    latest_date = bronze.agg(F.max(SNAPSHOT)).collect()[0][0]
    seed = spark.createDataFrame(
        [(ENTITY, latest_date, None, "COMMITTED")],
        "entity string, last_processed_snapshot date, candidate_snapshot date, status string",
    ).withColumn("updated_at", F.current_timestamp())

    (DeltaTable.forName(spark, CONTROL_TABLE).alias("t")
        .merge(seed.alias("s"), "t.entity = s.entity")
        .whenNotMatchedInsertAll()
        .execute())

    print(f"✅ Watermark inicializado após prova de equivalência do backfill: {latest_date}")
    return get_watermark_row()


def resolve_valid_from(events, preferred_col: str, first_version: bool):
    snapshot_ts = F.col(SNAPSHOT).cast("timestamp")
    preferred_ts = parse_erp_timestamp(preferred_col)

    with_candidate = (
        events.withColumn("_snapshot_ts", snapshot_ts)
              .withColumn("_preferred_ts", preferred_ts)
              .withColumn(
                  "_candidate_valid_from",
                  F.when(
                      F.col("_preferred_ts").isNotNull()
                      & (F.col("_preferred_ts") <= F.col("_snapshot_ts")),
                      F.col("_preferred_ts"),
                  ).otherwise(F.col("_snapshot_ts")),
              )
              .withColumn(
                  "_candidate_source",
                  F.when(
                      F.col("_preferred_ts").isNotNull()
                      & (F.col("_preferred_ts") <= F.col("_snapshot_ts")),
                      F.lit(preferred_col),
                  ).otherwise(F.lit("ingestion_date")),
              )
    )

    target = spark.table(SILVER)
    exact = target.select(
        F.col(KEY).alias("_existing_id"),
        F.col("valid_from").alias("_existing_valid_from"),
        F.col("hash_versao").alias("_existing_hash"),
    )
    target_max = target.groupBy(KEY).agg(F.max("valid_from").alias("_max_valid_from"))

    joined = (
        with_candidate.alias("s")
        .join(
            exact.alias("e"),
            (F.col(f"s.{KEY}") == F.col("e._existing_id"))
            & (F.col("s._candidate_valid_from") == F.col("e._existing_valid_from")),
            "left",
        )
        .join(target_max, on=KEY, how="left")
    )

    # Se a versão já foi aplicada em uma execução anterior, o mesmo grain precisa
    # carregar o mesmo hash. Caso contrário, parar é mais seguro que sobrescrever história.
    collisions = joined.filter(
        F.col("_existing_valid_from").isNotNull()
        & ~F.col("_existing_hash").eqNullSafe(F.col("_hash"))
    ).count()
    if collisions:
        raise Exception(
            f"Colisão SCD2: {collisions:,} evento(s) tentam reutilizar (id, valid_from) com hash diferente"
        )

    resolved = (
        joined.withColumn(
            "valid_from",
            F.when(F.col("_existing_valid_from").isNotNull(), F.col("_candidate_valid_from"))
             .when(F.col("_max_valid_from").isNull(), F.col("_candidate_valid_from"))
             .when(F.col("_candidate_valid_from") > F.col("_max_valid_from"), F.col("_candidate_valid_from"))
             .when(F.col("_snapshot_ts") > F.col("_max_valid_from"), F.col("_snapshot_ts"))
             .otherwise(F.lit(None).cast("timestamp")),
        )
        .withColumn(
            "valid_from_source",
            F.when(F.col("_existing_valid_from").isNotNull(), F.col("_candidate_source"))
             .when(F.col("_max_valid_from").isNull(), F.col("_candidate_source"))
             .when(F.col("_candidate_valid_from") > F.col("_max_valid_from"), F.col("_candidate_source"))
             .otherwise(F.lit("ingestion_date")),
        )
    )

    unresolved = resolved.filter(F.col("valid_from").isNull()).count()
    if unresolved:
        kind = "primeira versão" if first_version else "mudança Type 2"
        raise Exception(f"Não foi possível ordenar temporalmente {unresolved:,} evento(s) de {kind}")

    helper = [
        "_snapshot_ts", "_preferred_ts", "_candidate_valid_from", "_candidate_source",
        "_existing_id", "_existing_valid_from", "_existing_hash", "_max_valid_from",
    ]
    return resolved.drop(*helper)


def close_previous_versions(events):
    if events.limit(1).count() == 0:
        return 0

    target = spark.table(SILVER).alias("t")
    event_keys = events.select(
        F.col(KEY).alias("_event_id"),
        F.col("valid_from").alias("_event_valid_from"),
    ).distinct()

    candidates = (
        target.join(
            event_keys.alias("e"),
            (F.col(f"t.{KEY}") == F.col("e._event_id"))
            & (F.col("t.valid_from") < F.col("e._event_valid_from")),
            "inner",
        )
        .select(
            F.col(f"t.{KEY}").alias(KEY),
            F.col("t.valid_from").alias("prior_valid_from"),
            F.col("e._event_valid_from").alias("new_valid_from"),
        )
    )

    w = Window.partitionBy(KEY, "new_valid_from").orderBy(F.col("prior_valid_from").desc())
    prior = candidates.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    missing_prior = (
        event_keys.join(
            prior.select(F.col(KEY).alias("_prior_id"), F.col("new_valid_from").alias("_prior_new_valid_from")),
            (F.col("_event_id") == F.col("_prior_id"))
            & (F.col("_event_valid_from") == F.col("_prior_new_valid_from")),
            "left",
        )
        .filter(F.col("_prior_id").isNull())
        .count()
    )
    if missing_prior:
        raise Exception(f"{missing_prior:,} mudança(s) Type 2 sem versão anterior para fechar")

    (DeltaTable.forName(spark, SILVER).alias("t")
        .merge(
            prior.alias("s"),
            f"t.{KEY} = s.{KEY} AND t.valid_from = s.prior_valid_from",
        )
        .whenMatchedUpdate(set={
            "valid_to": "s.new_valid_from",
            "is_current": "false",
        })
        .execute())

    return prior.count()


def insert_versions(events, silver_columns):
    if events.limit(1).count() == 0:
        return 0

    prepared = (
        events.withColumn("hash_versao", F.col("_hash"))
              .withColumn("valid_to", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True))
              .withColumn("scd_source_snapshot", F.col(SNAPSHOT).cast("date"))
              .drop("_hash")
    )

    missing_cols = [c for c in silver_columns if c not in prepared.columns]
    if missing_cols:
        raise Exception(f"Insert incremental não consegue reproduzir o schema Silver. Faltam: {missing_cols}")

    prepared = prepared.select(*silver_columns)

    before = spark.table(SILVER).count()
    (DeltaTable.forName(spark, SILVER).alias("t")
        .merge(
            prepared.alias("s"),
            f"t.{KEY} = s.{KEY} AND t.valid_from = s.valid_from",
        )
        .whenNotMatchedInsertAll()
        .execute())
    after = spark.table(SILVER).count()
    return after - before


def apply_type1(type1_changes):
    if type1_changes.limit(1).count() == 0:
        return 0

    src = type1_changes.select(KEY, *TYPE1_COLS).dropDuplicates([KEY])
    updates = {c: f"s.`{c}`" for c in TYPE1_COLS}

    (DeltaTable.forName(spark, SILVER).alias("t")
        .merge(src.alias("s"), f"t.{KEY} = s.{KEY}")
        .whenMatchedUpdate(set=updates)
        .execute())
    return src.count()


def process_snapshot(snapshot_date, previous_date, silver_columns):
    raw = spark.table(BRONZE)

    current = (
        raw.filter(F.col(SNAPSHOT) == F.lit(snapshot_date))
           .withColumn("_hash", type2_hash())
    )
    previous = (
        raw.filter(F.col(SNAPSHOT) == F.lit(previous_date))
           .withColumn("_hash", type2_hash())
    )

    duplicate_groups = current.groupBy(KEY).count().filter(F.col("count") > 1).count()
    if duplicate_groups:
        raise Exception(f"Snapshot {snapshot_date}: {duplicate_groups:,} id(s) duplicados")

    c = current.alias("c")
    p = previous.alias("p")
    joined = c.join(p, F.col(f"c.{KEY}") == F.col(f"p.{KEY}"), "left")

    current_cols = [F.col(f"c.{col}").alias(col) for col in current.columns]

    new_ids = joined.filter(F.col(f"p.{KEY}").isNull()).select(*current_cols)

    # Reaparecimento após ausência não recebe semântica inventada. A política atual
    # não fecha versões por ausência; portanto, esse caso precisa de modeling explícito.
    reappeared = new_ids.select(KEY).join(
        spark.table(SILVER).select(KEY).distinct(), on=KEY, how="inner"
    ).count()
    if reappeared:
        raise Exception(
            f"Snapshot {snapshot_date}: {reappeared:,} id(s) reapareceram após ausência. "
            "Política de reativação ainda não definida; processamento bloqueado."
        )

    type2_changes = joined.filter(
        F.col(f"p.{KEY}").isNotNull()
        & (F.col("c._hash") != F.col("p._hash"))
    ).select(*current_cols)

    t1_expr = changed_expr("c", "p", TYPE1_COLS)
    type1_changes = joined.filter(
        F.col(f"p.{KEY}").isNotNull() & t1_expr
    ).select(*[F.col(f"c.{col}").alias(col) for col in [KEY, *TYPE1_COLS]])

    disappeared = previous.select(KEY).join(current.select(KEY), on=KEY, how="left_anti").count()

    new_count = new_ids.count()
    type2_count = type2_changes.count()
    type1_count = type1_changes.select(KEY).distinct().count()

    print(f"\n--- Snapshot {snapshot_date} | anterior={previous_date} ---")
    print(f"novos ids:                {new_count:,}")
    print(f"mudanças Type 2:          {type2_count:,}")
    print(f"ids com mudança Type 1:   {type1_count:,}")
    print(f"ids ausentes no snapshot: {disappeared:,} (nenhuma ação por política)")

    # Primeira versão de IDs realmente novos.
    new_resolved = resolve_valid_from(new_ids, "datacadastro", first_version=True)
    new_inserted = insert_versions(new_resolved, silver_columns)

    # Mudanças Type 2: fecha a versão imediatamente anterior e insere a nova.
    changed_resolved = resolve_valid_from(type2_changes, "dataalteracao", first_version=False)
    closed = close_previous_versions(changed_resolved)
    changed_inserted = insert_versions(changed_resolved, silver_columns)

    # Type 1 é aplicado por último para que TODAS as versões, inclusive uma nova
    # versão Type 2 criada no mesmo snapshot, recebam o último valor conhecido.
    type1_updated_ids = apply_type1(type1_changes)

    print(f"versões anteriores fechadas: {closed:,}")
    print(f"primeiras versões inseridas:  {new_inserted:,}")
    print(f"novas versões Type 2:         {changed_inserted:,}")
    print(f"ids atualizados como Type 1:  {type1_updated_ids:,}")

    return {
        "new_ids": new_count,
        "type2_changes": type2_count,
        "type1_changes": type1_count,
        "disappeared": disappeared,
        "new_inserted": new_inserted,
        "changed_inserted": changed_inserted,
    }


ensure_dev()
ensure_control_table()

if not spark.catalog.tableExists(SILVER):
    raise Exception(
        f"{SILVER} não existe. Execute e aprove Gate B3/B4 antes da manutenção incremental."
    )

bronze = spark.table(BRONZE)
silver = spark.table(SILVER)
required = {KEY, SNAPSHOT, "datacadastro", "dataalteracao", *TYPE2_COLS, *TYPE1_COLS}
missing = sorted(required - set(bronze.columns))
if missing:
    raise Exception(f"Colunas obrigatórias ausentes em {BRONZE}: {missing}")

print("\n=== GATE B5 — INCREMENTAL PRODUCT SCD2 ===")
print(f"Bronze:  {BRONZE}")
print(f"Silver:  {SILVER}")
print(f"Control: {CONTROL_TABLE}")
print("Watermark do SCD2 é independente do watermark de extração do Pentaho.\n")

wm = seed_watermark_if_needed(bronze, silver)
committed = wm["last_processed_snapshot"]
latest_bronze = bronze.agg(F.max(SNAPSHOT)).collect()[0][0]

if committed is None:
    raise Exception("Watermark committed está NULL")
if committed > latest_bronze:
    raise Exception(
        f"Watermark {committed} está à frente da Bronze {latest_bronze}; controle inconsistente"
    )

new_snapshots = [
    r[SNAPSHOT]
    for r in (
        bronze.select(SNAPSHOT).distinct()
              .filter(F.col(SNAPSHOT) > F.lit(committed))
              .orderBy(SNAPSHOT)
              .collect()
    )
]

print(f"watermark committed: {committed}")
print(f"último snapshot Bronze: {latest_bronze}")
print(f"snapshots pendentes: {len(new_snapshots)}")

if not new_snapshots:
    print("✅ Nenhum snapshot novo. Silver permaneceu inalterada; watermark já representa o estado processado.")
else:
    silver_columns = spark.table(SILVER).columns
    snapshot_dates = [r[SNAPSHOT] for r in bronze.select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()]
    date_index = {d: i for i, d in enumerate(snapshot_dates)}

    totals = {
        "new_ids": 0,
        "type2_changes": 0,
        "type1_changes": 0,
        "disappeared": 0,
        "new_inserted": 0,
        "changed_inserted": 0,
    }

    for snapshot_date in new_snapshots:
        idx = date_index[snapshot_date]
        if idx == 0:
            raise Exception(f"Snapshot {snapshot_date} não possui snapshot anterior para comparação")
        previous_date = snapshot_dates[idx - 1]
        metrics = process_snapshot(snapshot_date, previous_date, silver_columns)
        for k, v in metrics.items():
            totals[k] += v

    candidate = new_snapshots[-1]
    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET candidate_snapshot = DATE '{candidate}',
            status = 'PENDING_VALIDATION',
            updated_at = current_timestamp()
        WHERE entity = '{ENTITY}'
    """)

    print("\n=== B5 APPLY CONCLUÍDO ===")
    print(f"snapshots processados:          {len(new_snapshots)}")
    print(f"novos ids observados:           {totals['new_ids']:,}")
    print(f"mudanças Type 2 observadas:     {totals['type2_changes']:,}")
    print(f"ids com mudança Type 1:         {totals['type1_changes']:,}")
    print(f"ausências observadas:           {totals['disappeared']:,} (sem fechamento automático)")
    print(f"candidate watermark:            {candidate}")
    print(f"committed watermark permanece:  {committed}")
    print("Próxima task: Quality Gate B4. Só depois dele o candidate vira committed.")
