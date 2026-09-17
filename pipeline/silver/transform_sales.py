# Databricks notebook source
# pipeline/silver/transform_sales.py
# Processa Bronze venda → Silver venda
# Cast de tipos, validação de contrato, quarentena e MERGE idempotente

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def job_param(nome: str, default: str) -> str:
    """Lê parâmetro do Job; mantém fallback para execução manual do notebook."""
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BUNDLE_FILES_PATH = job_param(
    "bundle_files_path",
    "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform",
)
CONTROL_ROOT = job_param("control_root", "s3://varejinho-lake/_control")

BRONZE     = f"{CATALOG}.bronze.venda"
SILVER     = f"{CATALOG}.silver.venda"
QUARENTENA = f"{CATALOG}.silver._quarantine_venda"
HISTORICO  = f"{CATALOG}.silver._quarantine_history_venda"
CONTRACT   = f"{BUNDLE_FILES_PATH}/contracts/silver/venda.yaml"
REGISTRY   = f"{CONTROL_ROOT}/schema_registry"


def detectar_drift(tabela, df, dbutils):
    schema_atual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    registry_file = f"{REGISTRY}/{tabela}.json"
    try:
        conteudo = dbutils.fs.head(registry_file)
        schema_anterior = json.loads(conteudo)
    except Exception:
        dbutils.fs.put(registry_file, json.dumps(schema_atual), overwrite=True)
        print(f"[{tabela}] Schema baseline criado em {REGISTRY}.")
        return

    novas     = sorted(set(schema_atual) - set(schema_anterior))
    removidas = sorted(set(schema_anterior) - set(schema_atual))
    alteradas = {c: {"antes": schema_anterior[c], "depois": schema_atual[c]}
                 for c in set(schema_atual) & set(schema_anterior)
                 if schema_anterior[c] != schema_atual[c]}

    if novas or removidas or alteradas:
        print(f"[DRIFT] {tabela}: novas={novas} removidas={removidas} alteradas={alteradas}")
    else:
        print(f"[{tabela}] Schema sem alterações.")

    dbutils.fs.put(registry_file, json.dumps(schema_atual), overwrite=True)


def validar_contrato(df, contract_path):
    import yaml
    with open(contract_path, "r") as f:
        contract = yaml.safe_load(f)

    columns = contract.get("columns", [])
    df = df.withColumn("_invalido", F.lit(False))
    df = df.withColumn("_motivo",   F.lit(""))

    for col_cfg in columns:
        col_name = col_cfg.get("name")
        nullable = col_cfg.get("nullable", True)
        min_val  = col_cfg.get("min", None)

        if col_name not in df.columns:
            continue

        if not nullable:
            df = df.withColumn("_invalido",
                F.when(F.col(col_name).isNull(), F.lit(True))
                .otherwise(F.col("_invalido")))
            df = df.withColumn("_motivo",
                F.when(F.col(col_name).isNull(),
                    F.concat(F.col("_motivo"), F.lit(f"|{col_name} nulo")))
                .otherwise(F.col("_motivo")))

        if min_val is not None:
            try:
                min_num = float(min_val)
                df = df.withColumn("_invalido",
                    F.when(F.col(col_name).cast("double") < min_num, F.lit(True))
                    .otherwise(F.col("_invalido")))
                df = df.withColumn("_motivo",
                    F.when(F.col(col_name).cast("double") < min_num,
                        F.concat(F.col("_motivo"), F.lit(f"|{col_name} < {min_val}")))
                    .otherwise(F.col("_motivo")))
            except (ValueError, TypeError):
                pass

    df_ok   = df.where(~F.col("_invalido")).drop("_invalido", "_motivo")
    df_quar = df.where( F.col("_invalido")).drop("_invalido")

    total     = df.count()
    invalidos = df_quar.count()
    relatorio = {
        "tabela":     contract.get("table", "venda"),
        "total":      total,
        "validos":    total - invalidos,
        "quarentena": invalidos,
    }
    return df_ok, df_quar, relatorio


# 1. Leitura da Bronze — incrementalidade será ligada ao watermark em gate próprio
df = spark.table(BRONZE)

# 2. Cast de tipos
df_typed = (df
    .withColumn("valortotal",
        F.regexp_replace(F.col("valortotal"), ",", ".").cast("decimal(14,2)"))
    .withColumn("quantidade",
        F.regexp_replace(F.col("quantidade"), ",", ".").cast("decimal(14,3)"))
    .withColumn("custocomimposto",
        F.regexp_replace(F.col("custocomimposto"), ",", ".").cast("decimal(14,3)"))
    .withColumn("custosemimposto",
        F.regexp_replace(F.col("custosemimposto"), ",", ".").cast("decimal(14,3)"))
    .withColumn("customediocomimposto",
        F.regexp_replace(F.col("customediocomimposto"), ",", ".").cast("decimal(14,3)"))
    .withColumn("customediosemimposto",
        F.regexp_replace(F.col("customediosemimposto"), ",", ".").cast("decimal(14,3)"))
    .withColumn("piscofins",
        F.regexp_replace(F.col("piscofins"), ",", ".").cast("decimal(14,3)"))
    .withColumn("piscofinscredito",
        F.regexp_replace(F.col("piscofinscredito"), ",", ".").cast("decimal(14,3)"))
    .withColumn("icmscredito",
        F.regexp_replace(F.col("icmscredito"), ",", ".").cast("decimal(14,3)"))
    .withColumn("icmsdebito",
        F.regexp_replace(F.col("icmsdebito"), ",", ".").cast("decimal(14,3)"))
    .withColumn("precovenda",
        F.regexp_replace(F.col("precovenda"), ",", ".").cast("decimal(14,3)"))
    .withColumn("data",
        F.to_timestamp(F.col("data"), "yyyy/MM/dd HH:mm:ss.SSS"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumnRenamed("valortotal", "valor_total")
)

# 3. Schema drift
detectar_drift("venda", df_typed, dbutils)

# 4. Contrato
df_ok, df_quar, relatorio = validar_contrato(df_typed, CONTRACT)
print(f"[venda] {relatorio}")

# 5. Deduplicação
w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
df_dedup = (df_ok
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn"))

# 6. MERGE idempotente
if spark.catalog.tableExists(SILVER):
    (DeltaTable.forName(spark, SILVER).alias("t")
        .merge(df_dedup.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    (df_dedup.write.format("delta")
        .partitionBy("ano", "mes")
        .saveAsTable(SILVER))

# 7. Quarentena — snapshot atual + histórico append-only
if spark.catalog.tableExists(QUARENTENA):
    spark.sql(f"TRUNCATE TABLE {QUARENTENA}")

if relatorio["quarentena"] > 0:
    (df_quar.write.format("delta")
        .mode("append")
        .saveAsTable(QUARENTENA))

    (df_quar
        .withColumn("_quarantined_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(HISTORICO))

    print(f"[venda] {relatorio['quarentena']} registros em quarentena nesta execução.")

count = spark.table(SILVER).count()
print(f"✅ {SILVER}: {count:,} linhas")
