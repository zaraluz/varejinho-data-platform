# databricks/silver/transform_venda.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import sys
sys.path.append("/Workspace/Users/<USER>/varejinho-data-platform")

from quality.contract_engine import ContractValidator
from databricks.silver.schema_drift import detectar_drift

BRONZE     = "varejinho.bronze.venda"
SILVER     = "varejinho.silver.venda"
QUARENTENA = "varejinho.silver._quarantine_venda"
CONTRACT   = "/Workspace/Users/<USER>/varejinho-data-platform/contracts/silver/venda.yaml"

def transformar_venda(spark, dbutils, ultima_particao: str = None):

    # 1. Leitura incremental da Bronze
    df = spark.table(BRONZE)
    if ultima_particao:
        df = df.where(F.col("ingestion_date") > F.lit(ultima_particao))

    if df.isEmpty():
        print("[venda] Nenhum dado novo na Bronze. Encerrando.")
        return


    # 2. Schema drift
    detectar_drift("venda", df, dbutils, spark)

    # 3. Cast de tipos
    df_typed = (
        df
        .withColumn("valortotal",
            F.regexp_replace(F.col("valortotal"), ",", ".").cast("decimal(14,2)"))
        .withColumn("quantidade",
            F.regexp_replace(F.col("quantidade"), ",", ".").cast("decimal(14,3)"))
        .withColumn("data",
            F.to_timestamp(F.col("data"), "yyyy/MM/dd HH:mm:ss.SSS"))
        .withColumn("ano",  F.year("data"))
        .withColumn("mes",  F.month("data"))
        # renomeia para bater com o contrato
        .withColumnRenamed("valortotal", "valor_total")
    )

    # 4. Contrato
    validator = ContractValidator(CONTRACT)
    df_ok, df_quar, relatorio = validator.validate(df_typed)
    print(f"[venda] {relatorio}")

    # 5. Deduplicação — mantém o registro mais recente por id
    w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
    df_dedup = (
        df_ok
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

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

    # Quarentena
    if relatorio["quarentena"] > 0:
        (df_quar.write.format("delta")
            .mode("append")
            .saveAsTable(QUARENTENA))
        print(f"[venda] {relatorio['quarentena']} registros em quarentena.")

transformar_venda(spark, dbutils)