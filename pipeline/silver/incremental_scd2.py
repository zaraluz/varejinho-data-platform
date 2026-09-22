# Databricks notebook source
# pipeline/silver/incremental_scd2.py
# Engine incremental SCD2 reutilizável para produto, fornecedor e mercadologico.
# Schema Drift é avaliado sobre uma representação Silver-shaped antes de qualquer MERGE.

import importlib.util

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


def resolve_bundle_files_path() -> str:
    explicit = job_param("bundle_files_path", "")
    if explicit:
        return explicit.rstrip("/")

    try:
        raw = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        workspace_path = raw if raw.startswith("/Workspace/") else f"/Workspace{raw}"
        marker = "/pipeline/silver/incremental_scd2"
        if marker in workspace_path:
            return workspace_path.split(marker, 1)[0]
    except Exception:
        pass

    return "/Workspace/Users/<USER>/varejinho-data-platform"


CATALOG = job_param("catalog", "varejinho_dev")
ENTITY = job_param("entity", "fornecedor")
BUNDLE_FILES_PATH = resolve_bundle_files_path()
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control/dev").rstrip("/")
KEY = "id"
SNAPSHOT = "ingestion_date"

CONFIG = {
    "produto": {
        "bronze": f"{CATALOG}.bronze.produto",
        "silver": f"{CATALOG}.silver.produto",
        "type2": [
            "descricaocompleta", "mercadologico1", "mercadologico2",
            "mercadologico3", "ncm1", "id_tipoembalagem",
        ],
        "type1": [
            "descricaoreduzida", "id_tipomercadoria", "pesoliquido", "pesobruto",
        ],
        "initial_valid_from": "datacadastro",
        "initial_compare": "timestamp",
        "change_valid_from": "dataalteracao",
        "change_compare": "timestamp",
        "identity_watch": [],
    },
    "fornecedor": {
        "bronze": f"{CATALOG}.bronze.fornecedor",
        "silver": f"{CATALOG}.silver.fornecedor",
        "type2": ["cnpj", "razaosocial"],
        "type1": [
            "nomefantasia",
            "id_situacaocadastro",
            "id_tipoempresa",
            "permitenfsempedido",
            "id_tipocustocompra",
            "id_tipocustodevolucaotroca",
            "pedidominimoqtd",
            "pedidominimovalor",
            "valormaximoverbapedido",
            "id_contacontabilfinanceiro",
            "id_fornecedorfavorecido",
            "id_municipio",
        ],
        "initial_valid_from": "datacadastro",
        "initial_compare": "date",
        "change_valid_from": None,  # primeiro snapshot em que a mudança foi observada
        "change_compare": "snapshot",
        "identity_watch": ["cnpj"],
    },
    "mercadologico": {
        "bronze": f"{CATALOG}.bronze.mercadologico",
        "silver": f"{CATALOG}.silver.mercadologico",
        # Estrutura do nó: mover de pai/caminho ou trocar de nível muda a classificação histórica.
        "type2": [
            "mercadologico1", "mercadologico2", "mercadologico3",
            "mercadologico4", "mercadologico5", "nivel",
        ],
        # Rótulo atual: renomear/corrigir descrição não cria versão histórica.
        "type1": ["descricao"],
        # A fonte não possui data de criação/alteração: só sabemos quando o estado foi observado.
        "initial_valid_from": None,
        "initial_compare": "snapshot",
        "change_valid_from": None,
        "change_compare": "snapshot",
        "identity_watch": [],
    },
}

if ENTITY not in CONFIG:
    raise Exception(f"Entidade SCD2 não configurada: {ENTITY}. Opções: {sorted(CONFIG)}")

CFG = CONFIG[ENTITY]
BRONZE = job_param("bronze_table", CFG["bronze"])
SILVER = job_param("silver_table", CFG["silver"])
CONTROL_TABLE = job_param("control_table", f"{CATALOG}.control.scd2_watermark")
CONTROL_SCHEMA = ".".join(CONTROL_TABLE.split(".")[:2])

if not CATALOG.endswith("_dev"):
    raise Exception(f"incremental_scd2 só pode executar em *_dev durante hardening. Recebido: {CATALOG}")

DRIFT_RUNTIME_PATH = f"{BUNDLE_FILES_PATH}/quality/schema_drift_runtime.py"
_drift_spec = importlib.util.spec_from_file_location(
    "varejinho_schema_drift_runtime_scd2", DRIFT_RUNTIME_PATH
)
if _drift_spec is None or _drift_spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema drift runtime: {DRIFT_RUNTIME_PATH}")
_drift_module = importlib.util.module_from_spec(_drift_spec)
_drift_spec.loader.exec_module(_drift_module)
SilverSchemaDriftRuntime = _drift_module.SilverSchemaDriftRuntime
DRIFT = SilverSchemaDriftRuntime(
    dbutils=dbutils,
    control_root=CONTROL_ROOT,
    bundle_files_path=BUNDLE_FILES_PATH,
)

bronze = spark.table(BRONZE)
source_cols = set(bronze.columns)
TYPE2_COLS = CFG["type2"]

if CFG["type1"] == "all_remaining":
    exclude = {
        KEY, SNAPSHOT, "datacadastro", "dataalteracao", "_metadata", *TYPE2_COLS
    }
    TYPE1_COLS = [c for c in bronze.columns if c not in exclude]
else:
    TYPE1_COLS = list(CFG["type1"])

required = {KEY, SNAPSHOT, *TYPE2_COLS, *TYPE1_COLS}
if CFG["initial_valid_from"]:
    required.add(CFG["initial_valid_from"])
if CFG["change_valid_from"]:
    required.add(CFG["change_valid_from"])
missing = sorted(required - source_cols)
if missing:
    raise Exception(f"Colunas obrigatórias ausentes em {BRONZE}: {missing}")


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
    return expr if expr is not None else F.lit(False)


def build_schema_drift_probe():
    """Representa o schema que o runtime SCD2 materializa na Silver.

    Colunas de negócio vêm da Bronze com os tipos observados. Somente as colunas
    técnicas geradas pelo runtime são adicionadas usando os tipos já aceitos na
    Silver. Assim additive/removed/type_change da origem continuam visíveis,
    sem comparar Bronze bruta diretamente contra um baseline que contém campos SCD2.
    """
    silver_fields = {field.name: field.dataType for field in spark.table(SILVER).schema.fields}
    probe = bronze.limit(0)
    generated = [
        "hash_versao",
        "valid_from",
        "valid_to",
        "is_current",
        "valid_from_source",
        "scd_source_snapshot",
    ]
    for column in generated:
        if column in silver_fields and column not in probe.columns:
            probe = probe.withColumn(column, F.lit(None).cast(silver_fields[column]))
    return probe


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
        raise Exception(f"Mais de uma linha de watermark para entity={ENTITY}")
    return rows[0] if rows else None


def latest_per_id(df):
    w = Window.partitionBy(KEY).orderBy(F.col(SNAPSHOT).desc())
    return df.withColumn("_rn_latest", F.row_number().over(w)).filter(F.col("_rn_latest") == 1)


def baseline_is_equivalent():
    silver = spark.table(SILVER)
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

    expected_ids = bronze.select(KEY).distinct().count()
    actual_ids = silver.select(KEY).distinct().count()
    if expected_ids != actual_ids:
        return False, f"ids divergentes: expected={expected_ids:,}, actual={actual_ids:,}"

    bad_current = (
        silver.groupBy(KEY)
              .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("_curr"))
              .filter(F.col("_curr") != 1)
              .count()
    )
    if bad_current:
        return False, f"{bad_current:,} id(s) sem exatamente uma versão current"

    latest = latest_per_id(bronze)

    latest_t2 = (
        latest.withColumn("_latest_hash", type2_hash())
              .select(KEY, "_latest_hash")
    )
    current_hash_mismatch = (
        silver.filter(F.col("is_current"))
              .select(KEY, "hash_versao")
              .join(latest_t2, on=KEY, how="full")
              .filter(~F.col("hash_versao").eqNullSafe(F.col("_latest_hash")))
              .count()
    )
    if current_hash_mismatch:
        return False, f"current Type 2 diverge em {current_hash_mismatch:,} id(s)"

    if TYPE1_COLS:
        latest_t1 = latest.select(
            KEY, *[F.col(c).alias(f"_latest_{c}") for c in TYPE1_COLS]
        )
        joined = silver.join(latest_t1, on=KEY, how="left")
        for c in TYPE1_COLS:
            mismatch = joined.filter(~F.col(c).eqNullSafe(F.col(f"_latest_{c}"))).count()
            if mismatch:
                return False, f"Type 1 {c} diverge em {mismatch:,} linha(s)"

    latest_snapshot = bronze.agg(F.max(SNAPSHOT)).collect()[0][0]
    return True, f"Silver equivalente à Bronze até {latest_snapshot}"


def seed_watermark_if_needed():
    row = get_watermark_row()
    if row:
        return row

    ok, detail = baseline_is_equivalent()
    if not ok:
        raise Exception(
            "Watermark inexistente e Silver não equivalente ao backfill; "
            f"não é seguro inicializar: {detail}"
        )

    latest_snapshot = bronze.agg(F.max(SNAPSHOT)).collect()[0][0]
    seed = spark.createDataFrame(
        [(ENTITY, latest_snapshot, None, "COMMITTED")],
        "entity string, last_processed_snapshot date, candidate_snapshot date, status string",
    ).withColumn("updated_at", F.current_timestamp())

    (
        DeltaTable.forName(spark, CONTROL_TABLE).alias("t")
        .merge(seed.alias("s"), "t.entity = s.entity")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ Watermark inicializado após prova de equivalência: {latest_snapshot}")
    return get_watermark_row()


def resolve_valid_from(events, preferred_col, compare_mode: str, first_version: bool):
    snapshot_ts = F.col(SNAPSHOT).cast("timestamp")

    if preferred_col is None:
        with_candidate = (
            events.withColumn("_snapshot_ts", snapshot_ts)
                  .withColumn("_candidate_valid_from", snapshot_ts)
                  .withColumn("_candidate_source", F.lit("ingestion_date"))
        )
    else:
        preferred_ts = parse_erp_timestamp(preferred_col)
        if compare_mode == "date":
            trusted = (
                F.col("_preferred_ts").isNotNull()
                & (F.to_date(F.col("_preferred_ts")) <= F.col(SNAPSHOT).cast("date"))
            )
        elif compare_mode == "timestamp":
            trusted = (
                F.col("_preferred_ts").isNotNull()
                & (F.col("_preferred_ts") <= F.col("_snapshot_ts"))
            )
        else:
            raise Exception(f"compare_mode inválido: {compare_mode}")

        with_candidate = (
            events.withColumn("_snapshot_ts", snapshot_ts)
                  .withColumn("_preferred_ts", preferred_ts)
                  .withColumn(
                      "_candidate_valid_from",
                      F.when(trusted, F.col("_preferred_ts")).otherwise(F.col("_snapshot_ts")),
                  )
                  .withColumn(
                      "_candidate_source",
                      F.when(trusted, F.lit(preferred_col)).otherwise(F.lit("ingestion_date")),
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

    collisions = joined.filter(
        F.col("_existing_valid_from").isNotNull()
        & ~F.col("_existing_hash").eqNullSafe(F.col("_hash"))
    ).count()
    if collisions:
        raise Exception(
            f"Colisão SCD2: {collisions:,} evento(s) reutilizam (id, valid_from) com hash diferente"
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
    return resolved.drop(*[c for c in helper if c in resolved.columns])


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
            prior.select(
                F.col(KEY).alias("_prior_id"),
                F.col("new_valid_from").alias("_prior_new_valid_from"),
            ),
            (F.col("_event_id") == F.col("_prior_id"))
            & (F.col("_event_valid_from") == F.col("_prior_new_valid_from")),
            "left",
        )
        .filter(F.col("_prior_id").isNull())
        .count()
    )
    if missing_prior:
        raise Exception(f"{missing_prior:,} mudança(s) Type 2 sem versão anterior para fechar")

    (
        DeltaTable.forName(spark, SILVER).alias("t")
        .merge(
            prior.alias("s"),
            f"t.{KEY} = s.{KEY} AND t.valid_from = s.prior_valid_from",
        )
        .whenMatchedUpdate(set={
            "valid_to": "s.new_valid_from",
            "is_current": "false",
        })
        .execute()
    )
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
        raise Exception(f"Insert incremental não reproduz schema Silver. Faltam: {missing_cols}")

    # A política de additive já foi aplicada no preflight. Esta projeção mantém a
    # Silver no baseline aceito até uma promoção explícita do schema.
    prepared = prepared.select(*silver_columns)
    before = spark.table(SILVER).count()
    (
        DeltaTable.forName(spark, SILVER).alias("t")
        .merge(
            prepared.alias("s"),
            f"t.{KEY} = s.{KEY} AND t.valid_from = s.valid_from",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return spark.table(SILVER).count() - before


def apply_type1(type1_changes):
    if not TYPE1_COLS or type1_changes.limit(1).count() == 0:
        return 0

    src = type1_changes.select(KEY, *TYPE1_COLS).dropDuplicates([KEY])
    updates = {c: f"s.`{c}`" for c in TYPE1_COLS}

    (
        DeltaTable.forName(spark, SILVER).alias("t")
        .merge(src.alias("s"), f"t.{KEY} = s.{KEY}")
        .whenMatchedUpdate(set=updates)
        .execute()
    )
    return src.count()


def process_snapshot(snapshot_date, previous_date, silver_columns):
    raw = spark.table(BRONZE)
    current = raw.filter(F.col(SNAPSHOT) == F.lit(snapshot_date)).withColumn("_hash", type2_hash())
    previous = raw.filter(F.col(SNAPSHOT) == F.lit(previous_date)).withColumn("_hash", type2_hash())

    duplicate_groups = current.groupBy(KEY).count().filter(F.col("count") > 1).count()
    if duplicate_groups:
        raise Exception(f"Snapshot {snapshot_date}: {duplicate_groups:,} id(s) duplicados")

    c = current.alias("c")
    p = previous.alias("p")
    joined = c.join(p, F.col(f"c.{KEY}") == F.col(f"p.{KEY}"), "left")
    current_cols = [F.col(f"c.{col}").alias(col) for col in current.columns]

    new_ids = joined.filter(F.col(f"p.{KEY}").isNull()).select(*current_cols)

    existing_seen = (
        spark.table(SILVER)
             .groupBy(KEY)
             .agg(F.max("scd_source_snapshot").alias("_max_scd_source_snapshot"))
             .withColumn("_already_in_silver", F.lit(True))
    )
    classified_new = new_ids.join(existing_seen, on=KEY, how="left")

    already_applied_new = classified_new.filter(
        F.col("_already_in_silver").isNotNull()
        & F.col("_max_scd_source_snapshot").isNotNull()
        & (F.col("_max_scd_source_snapshot") >= F.lit(snapshot_date))
    ).count()

    true_reappeared = classified_new.filter(
        F.col("_already_in_silver").isNotNull()
        & (
            F.col("_max_scd_source_snapshot").isNull()
            | (F.col("_max_scd_source_snapshot") < F.lit(snapshot_date))
        )
    ).count()
    if true_reappeared:
        raise Exception(
            f"Snapshot {snapshot_date}: {true_reappeared:,} id(s) reapareceram após ausência; "
            "política de reativação ainda não definida."
        )

    new_ids = classified_new.drop("_max_scd_source_snapshot", "_already_in_silver")

    type2_changes = joined.filter(
        F.col(f"p.{KEY}").isNotNull()
        & (F.col("c._hash") != F.col("p._hash"))
    ).select(*current_cols)

    t1_expr = changed_expr("c", "p", TYPE1_COLS)
    type1_changes = joined.filter(
        F.col(f"p.{KEY}").isNotNull() & t1_expr
    ).select(*[F.col(f"c.{col}").alias(col) for col in [KEY, *TYPE1_COLS]])

    disappeared = previous.select(KEY).join(current.select(KEY), on=KEY, how="left_anti").count()

    identity_alerts = {}
    for watched in CFG["identity_watch"]:
        alerts = joined.filter(
            F.col(f"p.{KEY}").isNotNull()
            & (~F.col(f"c.{watched}").eqNullSafe(F.col(f"p.{watched}")))
        ).count()
        identity_alerts[watched] = alerts
        if alerts:
            print(f"⚠️ ALERTA DE IDENTIDADE: {alerts:,} mudança(s) em {watched} no snapshot {snapshot_date}")

    print(f"\n--- {ENTITY} | Snapshot {snapshot_date} | anterior={previous_date} ---")
    print(f"novos ids no delta Bronze:      {new_ids.count():,}")
    print(f"novos ids já aplicados/replay:  {already_applied_new:,}")
    print(f"mudanças Type 2:                {type2_changes.count():,}")
    print(f"ids com mudança Type 1:         {type1_changes.select(KEY).distinct().count():,}")
    print(f"ids ausentes no snapshot:       {disappeared:,} (nenhuma ação por política)")

    new_resolved = resolve_valid_from(
        new_ids,
        CFG["initial_valid_from"],
        CFG["initial_compare"],
        first_version=True,
    )
    new_inserted = insert_versions(new_resolved, silver_columns)

    changed_resolved = resolve_valid_from(
        type2_changes,
        CFG["change_valid_from"],
        CFG["change_compare"],
        first_version=False,
    )
    closed = close_previous_versions(changed_resolved)
    changed_inserted = insert_versions(changed_resolved, silver_columns)

    type1_updated_ids = apply_type1(type1_changes)

    print(f"versões anteriores matched/fechadas: {closed:,}")
    print(f"primeiras versões inseridas:         {new_inserted:,}")
    print(f"novas versões Type 2 inseridas:      {changed_inserted:,}")
    print(f"ids matched para update Type 1:      {type1_updated_ids:,}")

    return {
        "new_ids": new_ids.count(),
        "already_applied_new": already_applied_new,
        "type2_changes": type2_changes.count(),
        "type1_changes": type1_changes.select(KEY).distinct().count(),
        "disappeared": disappeared,
        "new_inserted": new_inserted,
        "changed_inserted": changed_inserted,
        "identity_alerts": sum(identity_alerts.values()),
    }


if not spark.catalog.tableExists(SILVER):
    raise Exception(f"{SILVER} não existe. Execute o backfill/Quality Gate antes do incremental.")

# Schema Drift precisa ser decidido antes de qualquer MERGE SCD2. O probe usa
# tipos observados da Bronze + somente as colunas técnicas geradas pelo runtime.
# Assim não comparamos Bronze bruta diretamente contra um baseline Silver.
drift_probe = build_schema_drift_probe()
_, drift_report = DRIFT.evaluate(ENTITY, drift_probe)

ensure_control_table()

print("\n=== GENERIC INCREMENTAL SCD2 ===")
print(f"Entity:  {ENTITY}")
print(f"Bronze:  {BRONZE}")
print(f"Silver:  {SILVER}")
print(f"Control: {CONTROL_TABLE}")
print(f"Control root: {CONTROL_ROOT}")
print(f"Bundle files path: {BUNDLE_FILES_PATH}")
print(f"Schema Drift: {drift_report['classification']} | action={drift_report['action']}")
print(f"Type 2:  {TYPE2_COLS}")
print(f"Type 1:  {len(TYPE1_COLS)} atributo(s)")
print(f"Initial boundary: {CFG['initial_valid_from']}")
print(f"Change boundary:  {CFG['change_valid_from'] or 'ingestion_date'}\n")

wm = seed_watermark_if_needed()
committed = wm["last_processed_snapshot"]
latest_bronze = bronze.agg(F.max(SNAPSHOT)).collect()[0][0]

if committed is None:
    raise Exception("Watermark committed está NULL")
if committed > latest_bronze:
    raise Exception(f"Watermark {committed} está à frente da Bronze {latest_bronze}")

new_snapshots = [
    r[SNAPSHOT]
    for r in (
        bronze.select(SNAPSHOT).distinct()
              .filter(F.col(SNAPSHOT) > F.lit(committed))
              .orderBy(SNAPSHOT)
              .collect()
    )
]

print(f"watermark committed:   {committed}")
print(f"último snapshot Bronze: {latest_bronze}")
print(f"snapshots pendentes:   {len(new_snapshots)}")

if not new_snapshots:
    print("✅ Nenhum snapshot novo. Silver permaneceu inalterada.")
else:
    silver_columns = spark.table(SILVER).columns
    snapshot_dates = [r[SNAPSHOT] for r in bronze.select(SNAPSHOT).distinct().orderBy(SNAPSHOT).collect()]
    date_index = {d: i for i, d in enumerate(snapshot_dates)}

    totals = {
        "new_ids": 0, "already_applied_new": 0, "type2_changes": 0,
        "type1_changes": 0, "disappeared": 0, "new_inserted": 0,
        "changed_inserted": 0, "identity_alerts": 0,
    }

    for snapshot_date in new_snapshots:
        idx = date_index[snapshot_date]
        if idx == 0:
            raise Exception(f"Snapshot {snapshot_date} não possui snapshot anterior")
        metrics = process_snapshot(snapshot_date, snapshot_dates[idx - 1], silver_columns)
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

    print("\n=== SCD2 APPLY CONCLUÍDO ===")
    print(f"snapshots processados:           {len(new_snapshots)}")
    print(f"novos ids observados:            {totals['new_ids']:,}")
    print(f"mudanças Type 2 observadas:      {totals['type2_changes']:,}")
    print(f"ids com mudança Type 1:          {totals['type1_changes']:,}")
    print(f"alertas de identidade:           {totals['identity_alerts']:,}")
    print(f"candidate watermark:             {candidate}")
    print(f"committed permanece:             {committed}")
    print("Próxima task obrigatória: Quality Gate; só depois commit do watermark.")
