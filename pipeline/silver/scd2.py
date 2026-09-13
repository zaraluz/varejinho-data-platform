# databricks/silver/scd2.py
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Colunas de negócio que definem uma "versão" de cada dimensão
SCD2_CONFIG = {
    "produto": [
        "descricaocompleta",
        "descricaoreduzida",
        "mercadologico1",
        "mercadologico2",
        "mercadologico3",
        "ncm1",
    ],
    "fornecedor": [
        "razaosocial",
        "nomefantasia",
        "cnpj",
        "id_situacaocadastro",
    ],
    "mercadologico": [
        "descricao",
        "mercadologico1",
        "mercadologico2",
        "mercadologico3",
        "nivel",
    ],
}

# Dimensões com SCD Tipo 1 — overwrite simples, sem histórico
SCD1_TABELAS = [
    "loja",
    "produtofornecedor",
    "tipocurvaabc",
    "tipomotivoperda",
    "tipopedido",
    "tipopromocao",
]


def aplicar_scd2(tabela: str, spark):
    """
    Aplica SCD Tipo 2 para dimensões mutáveis.
    Fecha a versão anterior quando o hash muda.
    Insere nova versão com valid_from = agora.
    """
    cols = SCD2_CONFIG[tabela]
    BRONZE = f"varejinho.bronze.{tabela}"
    SILVER = f"varejinho.silver.{tabela}"

    df_novo = (
        spark.table(BRONZE)
        # Só a última partição (full load — pega o estado atual)
        .where(F.col("ingestion_date") == spark.table(BRONZE)
               .agg(F.max("ingestion_date")).collect()[0][0])
        .withColumn(
            "hash_versao",
            F.md5(F.concat_ws("||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
                  for c in cols]
            ))
        )
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to",   F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(SILVER):
        # Primeira carga — grava direto
        df_novo.write.format("delta").saveAsTable(SILVER)
        print(f"[{tabela}] SCD2 — carga inicial: {df_novo.count()} registros.")
        return

    t = DeltaTable.forName(spark, SILVER)

    # Passo 1 — fecha versões que mudaram (hash diferente)
    (t.alias("t")
        .merge(df_novo.alias("s"), "t.id = s.id AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.hash_versao <> s.hash_versao",
            set={
                "valid_to":   "s.valid_from",
                "is_current": "false"
            }
        )
        .whenNotMatchedInsertAll()
        .execute())

    # Passo 2 — insere novas versões dos registros fechados
    fechados = (
        spark.table(SILVER)
        .where("is_current = false")
        .select("id", "hash_versao")
        .distinct()
    )

    df_inserir = (
        df_novo
        .join(fechados.withColumnRenamed("hash_versao", "_hash_old"), "id", "inner")
        .where(F.col("hash_versao") != F.col("_hash_old"))
        .drop("_hash_old")
    )

    if df_inserir.count() > 0:
        df_inserir.write.format("delta").mode("append").saveAsTable(SILVER)
        print(f"[{tabela}] SCD2 — {df_inserir.count()} novas versoes inseridas.")


def aplicar_scd1(tabela: str, spark):
    """
    SCD Tipo 1 — overwrite simples para domínios e tabelas estáticas.
    Não preserva histórico.
    """
    BRONZE = f"varejinho.bronze.{tabela}"
    SILVER = f"varejinho.silver.{tabela}"

    df = (
        spark.table(BRONZE)
        .where(F.col("ingestion_date") == spark.table(BRONZE)
               .agg(F.max("ingestion_date")).collect()[0][0])
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(SILVER))

    print(f"[{tabela}] SCD1 — {df.count()} registros gravados.")


# Rodar todas as dimensões
if __name__ == "__main__":
    for tabela in SCD2_CONFIG:
        aplicar_scd2(tabela, spark)

    for tabela in SCD1_TABELAS:
        aplicar_scd1(tabela, spark)