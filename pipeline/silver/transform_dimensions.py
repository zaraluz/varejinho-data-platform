# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def job_param(nome: str, default: str) -> str:
    """Lê parâmetro do Job; mantém fallback para execução manual do notebook."""
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")

SCD2_CONFIG = {
    "produto":       {
        "cols": ["descricaocompleta","descricaoreduzida","mercadologico1","mercadologico2","mercadologico3","ncm1"],
        "valid_from_col": "datacadastro",
    },
    "fornecedor":    {
        "cols": ["razaosocial","nomefantasia","cnpj","id_situacaocadastro"],
        "valid_from_col": "datacadastro",
    },
    "mercadologico": {
        "cols": ["descricao","mercadologico1","mercadologico2","mercadologico3","nivel"],
        "valid_from_col": None,
    },
}

SCD1_TABELAS = [
    "loja", "produtofornecedor", "tipocurvaabc", "tipomotivoperda", "tipopedido", "tipopromocao",
    "situacaocadastro", "situacaonotaentrada", "situacaopagarfornecedorparcela",
    "situacaopagaroutrasdespesas", "situacaopedido", "tipoembalagem", "tipoentrada",
    "tipofornecedor", "tipomercadoria", "tipomovimentacao", "tipooferta",
    "tipopagamento", "tipoplanoconta",
]

resultados = []

# SCD2
for tabela, cfg in SCD2_CONFIG.items():
    try:
        BRONZE = f"{CATALOG}.bronze.{tabela}"
        SILVER = f"{CATALOG}.silver.{tabela}"

        ultima = spark.table(BRONZE).agg(F.max("ingestion_date")).collect()[0][0]
        df_bronze = spark.table(BRONZE).where(F.col("ingestion_date") == ultima)

        # valid_from: data real do ERP ou fallback 2020-01-01
        if cfg["valid_from_col"]:
            df_bronze = df_bronze.withColumn("valid_from",
                F.to_timestamp(F.col(cfg["valid_from_col"]), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"))
        else:
            df_bronze = df_bronze.withColumn("valid_from",
                F.lit("2020-01-01").cast("timestamp"))

        df_novo = (df_bronze
            .withColumn("hash_versao",
                F.md5(F.concat_ws("||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cfg["cols"]])))
            .withColumn("valid_to",   F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True)))

        # Implementação SCD2 será substituída no próximo gate.
        # Por enquanto a mudança deste commit é apenas isolamento dev/prod.
        spark.sql(f"DROP TABLE IF EXISTS {SILVER}")
        df_novo.write.format("delta").saveAsTable(SILVER)
        count = spark.table(SILVER).count()
        resultados.append(f"✅ {tabela} SCD2 reprocessado: {count:,}")

    except Exception as e:
        resultados.append(f"❌ {tabela}: {str(e)[:150]}")

# SCD1
for tabela in SCD1_TABELAS:
    try:
        BRONZE = f"{CATALOG}.bronze.{tabela}"
        SILVER = f"{CATALOG}.silver.{tabela}"
        ultima = spark.table(BRONZE).agg(F.max("ingestion_date")).collect()[0][0]
        df = spark.table(BRONZE).where(F.col("ingestion_date") == ultima)
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema","true").saveAsTable(SILVER))
        count = spark.table(SILVER).count()
        resultados.append(f"✅ {tabela} SCD1: {count:,}")
    except Exception as e:
        resultados.append(f"❌ {tabela}: {str(e)[:150]}")

# CURVAABC — fato snapshot
try:
    BRONZE = f"{CATALOG}.bronze.curvaabc"
    SILVER = f"{CATALOG}.silver.curvaabc"

    df = spark.table(BRONZE)
    df_typed = (df
        .withColumn("id",           F.col("id").cast("bigint"))
        .withColumn("id_loja",      F.col("id_loja").cast("int"))
        .withColumn("id_produto",   F.col("id_produto").cast("int"))
        .withColumn("quantidade",   F.regexp_replace(F.col("quantidade"), ",", ".").cast("decimal(14,3)"))
        .withColumn("valortotal",   F.regexp_replace(F.col("valortotal"), ",", ".").cast("decimal(14,2)"))
        .withColumn("lucro",        F.regexp_replace(F.col("lucro"),      ",", ".").cast("decimal(14,2)"))
        .withColumn("id_tipocurvaabc_nivel1", F.col("id_tipocurvaabc_nivel1").cast("int"))
        .withColumn("id_tipocurvaabc_nivel2", F.col("id_tipocurvaabc_nivel2").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico1_nivel1", F.col("id_tipocurvaabcmercadologico1_nivel1").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico1_nivel2", F.col("id_tipocurvaabcmercadologico1_nivel2").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico2_nivel1", F.col("id_tipocurvaabcmercadologico2_nivel1").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico2_nivel2", F.col("id_tipocurvaabcmercadologico2_nivel2").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico3_nivel1", F.col("id_tipocurvaabcmercadologico3_nivel1").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico3_nivel2", F.col("id_tipocurvaabcmercadologico3_nivel2").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico4_nivel1", F.col("id_tipocurvaabcmercadologico4_nivel1").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico4_nivel2", F.col("id_tipocurvaabcmercadologico4_nivel2").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico5_nivel1", F.col("id_tipocurvaabcmercadologico5_nivel1").cast("int"))
        .withColumn("id_tipocurvaabcmercadologico5_nivel2", F.col("id_tipocurvaabcmercadologico5_nivel2").cast("int"))
        .withColumn("snapshot_date", F.col("ingestion_date").cast("date"))
    )

    if spark.catalog.tableExists(SILVER):
        (DeltaTable.forName(spark, SILVER).alias("t")
            .merge(
                df_typed.alias("s"),
                "t.id_produto = s.id_produto AND t.id_loja = s.id_loja AND t.snapshot_date = s.snapshot_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        (df_typed.write.format("delta")
            .partitionBy("snapshot_date")
            .saveAsTable(SILVER))

    count = spark.table(SILVER).count()
    snapshots = spark.table(SILVER).select("snapshot_date").distinct().count()
    produtos  = spark.table(SILVER).select("id_produto").distinct().count()
    resultados.append(f"✅ curvaabc snapshot: {count:,} linhas | {snapshots} snapshots | {produtos:,} produtos")

except Exception as e:
    resultados.append(f"❌ curvaabc: {str(e)[:150]}")

print("\n=== RESULTADO ===")
for r in resultados:
    print(r)
