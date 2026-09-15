# pipeline/silver/silver_scd2.py
# SCD Tipo 2 para dimensões mutáveis + SCD Tipo 1 para domínios + curvaabc snapshot
# ATENÇÃO: código inline nos notebooks do Databricks por limitação de imports em serverless
# Este arquivo é a versão versionada para o repositório

from pyspark.sql import functions as F
from delta.tables import DeltaTable

SCD2_CONFIG = {
    "produto":       ["descricaocompleta","descricaoreduzida","mercadologico1","mercadologico2","mercadologico3","ncm1"],
    "fornecedor":    ["razaosocial","nomefantasia","cnpj","id_situacaocadastro"],
    "mercadologico": ["descricao","mercadologico1","mercadologico2","mercadologico3","nivel"],
}

SCD1_TABELAS = [
    # Originais
    "loja", "produtofornecedor", "tipocurvaabc", "tipomotivoperda", "tipopedido", "tipopromocao",
    # Domínios adicionados
    "situacaocadastro", "situacaonotaentrada", "situacaopagarfornecedorparcela",
    "situacaopagaroutrasdespesas", "situacaopedido", "tipoembalagem", "tipoentrada",
    "tipofornecedor", "tipomercadoria", "tipomovimentacao", "tipooferta",
    "tipopagamento", "tipoplanoconta",
]

resultados = []

# SCD2
for tabela, cols in SCD2_CONFIG.items():
    try:
        BRONZE = f"varejinho.bronze.{tabela}"
        SILVER = f"varejinho.silver.{tabela}"

        ultima = spark.table(BRONZE).agg(F.max("ingestion_date")).collect()[0][0]
        df_novo = (spark.table(BRONZE)
            .where(F.col("ingestion_date") == ultima)
            .withColumn("hash_versao",
                F.md5(F.concat_ws("||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cols])))
            .withColumn("valid_from", F.current_timestamp())
            .withColumn("valid_to",   F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True)))

        if not spark.catalog.tableExists(SILVER):
            df_novo.write.format("delta").saveAsTable(SILVER)
            count = spark.table(SILVER).count()
            resultados.append(f"✅ {tabela} SCD2 carga inicial: {count:,}")
        else:
            t = DeltaTable.forName(spark, SILVER)
            (t.alias("t")
                .merge(df_novo.alias("s"), "t.id = s.id AND t.is_current = true")
                .whenMatchedUpdate(
                    condition="t.hash_versao <> s.hash_versao",
                    set={"valid_to": "s.valid_from", "is_current": "false"})
                .whenNotMatchedInsertAll()
                .execute())
            count = spark.table(SILVER).count()
            resultados.append(f"✅ {tabela} SCD2 atualizado: {count:,}")
    except Exception as e:
        resultados.append(f"❌ {tabela}: {str(e)[:120]}")

# SCD1
for tabela in SCD1_TABELAS:
    try:
        BRONZE = f"varejinho.bronze.{tabela}"
        SILVER = f"varejinho.silver.{tabela}"
        ultima = spark.table(BRONZE).agg(F.max("ingestion_date")).collect()[0][0]
        df = spark.table(BRONZE).where(F.col("ingestion_date") == ultima)
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema","true").saveAsTable(SILVER))
        count = spark.table(SILVER).count()
        resultados.append(f"✅ {tabela} SCD1: {count:,}")
    except Exception as e:
        resultados.append(f"❌ {tabela}: {str(e)[:120]}")

# CURVAABC — fato snapshot
try:
    BRONZE = "varejinho.bronze.curvaabc"
    SILVER = "varejinho.silver.curvaabc"

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
    resultados.append(f"❌ curvaabc: {str(e)[:120]}")

print("\n=== RESULTADO ===")
for r in resultados:
    print(r)