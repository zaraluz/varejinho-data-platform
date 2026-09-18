# Databricks notebook source
# pipeline/silver/transform_reference_dimensions.py
# Dimensões SCD1/domínios + curvaabc snapshot.
# IMPORTANTE: produto, fornecedor e mercadologico NÃO são tratados aqui.
# Esses três SCD2 pertencem ao runtime incremental_scd2.py.

from pyspark.sql import functions as F
from delta.tables import DeltaTable


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")

SCD1_TABELAS = [
    "loja", "produtofornecedor", "tipocurvaabc", "tipomotivoperda", "tipopedido", "tipopromocao",
    "situacaocadastro", "situacaonotaentrada", "situacaopagarfornecedorparcela",
    "situacaopagaroutrasdespesas", "situacaopedido", "tipoembalagem", "tipoentrada",
    "tipofornecedor", "tipomercadoria", "tipomovimentacao", "tipooferta",
    "tipopagamento", "tipoplanoconta",
]

resultados = []

print("\n=== SILVER — REFERENCE DIMENSIONS / SNAPSHOTS ===")
print(f"Catalog: {CATALOG}")
print("SCD2 excluídos deste notebook: produto, fornecedor, mercadologico\n")

# SCD1: estado corrente do último snapshot Bronze.
for tabela in SCD1_TABELAS:
    try:
        bronze = f"{CATALOG}.bronze.{tabela}"
        silver = f"{CATALOG}.silver.{tabela}"

        ultima = spark.table(bronze).agg(F.max("ingestion_date")).collect()[0][0]
        if ultima is None:
            raise Exception("Bronze sem ingestion_date válido")

        df = spark.table(bronze).where(F.col("ingestion_date") == F.lit(ultima))

        (
            df.write.format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(silver)
        )

        count = spark.table(silver).count()
        resultados.append(f"✅ {tabela} SCD1: {count:,} | snapshot={ultima}")

    except Exception as e:
        resultados.append(f"❌ {tabela}: {str(e)[:180]}")

# CURVAABC: histórico por snapshot.
try:
    bronze = f"{CATALOG}.bronze.curvaabc"
    silver = f"{CATALOG}.silver.curvaabc"

    df = spark.table(bronze)
    df_typed = (
        df.withColumn("id", F.col("id").cast("bigint"))
          .withColumn("id_loja", F.col("id_loja").cast("int"))
          .withColumn("id_produto", F.col("id_produto").cast("int"))
          .withColumn("quantidade", F.regexp_replace(F.col("quantidade"), ",", ".").cast("decimal(14,3)"))
          .withColumn("valortotal", F.regexp_replace(F.col("valortotal"), ",", ".").cast("decimal(14,2)"))
          .withColumn("lucro", F.regexp_replace(F.col("lucro"), ",", ".").cast("decimal(14,2)"))
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

    if spark.catalog.tableExists(silver):
        (
            DeltaTable.forName(spark, silver).alias("t")
            .merge(
                df_typed.alias("s"),
                "t.id_produto = s.id_produto "
                "AND t.id_loja = s.id_loja "
                "AND t.snapshot_date = s.snapshot_date",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df_typed.write.format("delta")
            .partitionBy("snapshot_date")
            .saveAsTable(silver)
        )

    out = spark.table(silver)
    count = out.count()
    snapshots = out.select("snapshot_date").distinct().count()
    produtos = out.select("id_produto").distinct().count()
    resultados.append(
        f"✅ curvaabc snapshot: {count:,} linhas | {snapshots} snapshots | {produtos:,} produtos"
    )

except Exception as e:
    resultados.append(f"❌ curvaabc: {str(e)[:180]}")

print("\n=== RESULTADO ===")
for r in resultados:
    print(r)

falhas = [r for r in resultados if r.startswith("❌")]
if falhas:
    raise Exception("Reference dimensions falharam:\n" + "\n".join(falhas))

print("\n✅ Reference dimensions concluídas sem tocar nas três dimensões SCD2.")
