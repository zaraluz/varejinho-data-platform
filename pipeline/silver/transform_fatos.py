# databricks/silver/transform_fatos.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import sys
sys.path.append("/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform")

from quality.contract_engine import ContractValidator
from databricks.silver.schema_drift import detectar_drift

REPO = "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform"

# Configuração por tabela: chave de dedup, coluna de data e decimais a converter
CONFIG = {
    "notaentrada": {
        "chave":    ["numeronota", "id_loja", "id_fornecedor"],
        "data":     "dataentrada",
        "decimais": ["valortotal", "valormercadoria", "valordesconto"],
    },
    "notaentradaitem": {
        "chave":    ["id"],
        "data":     None,
        "decimais": ["quantidade", "valor", "valortotal"],
    },
    "perda": {
        "chave":    ["id"],
        "data":     "data",
        "decimais": ["quantidade", "valor"],
    },
    "logestoque": {
        "chave":    ["id"],
        "data":     "datamovimento",
        "decimais": ["quantidade", "estoqueanterior", "estoqueatual", 
                    "custocomimposto", "custosemimposto", 
                    "customediocomimposto", "customediosemimposto"],
},
    "promocao": {
        "chave":    ["id"],
        "data":     "datainicio",
        "decimais": ["valor", "valordesconto"],
    },
    "promocaoitem": {
        "chave":    ["id"],
        "data":     None,
        "decimais": ["precovenda"],
    },
    "pedido": {
        "chave":    ["id"],
        "data":     "datacompra",
        "decimais": [],
    },
    "pedidoitem": {
        "chave":    ["id"],
        "data":     None,
        "decimais": ["quantidade", "custocompra", "valortotal"],
    },
    "oferta": {
        "chave":    ["id"],
        "data":     "datainicio",
        "decimais": ["precooferta", "preconormal", "precoimediato"],
    },
        "pagarfornecedor": {
        "chave":    ["id"],
        "data":     "dataemissao",
        "decimais": ["valor"],
    },
    "pagarfornecedorparcela": {
        "chave":    ["id"],
        "data":     "datavencimento",
        "decimais": ["valor", "valoracrescimo"],
        "datas_extras": ["datapagamento", "datapagamentocontabil"],
    },
    "pagaroutrasdespesas": {
        "chave":    ["id"],
        "data":     "dataemissao",
        "decimais": ["valor", "valorbruto"],
    },
    "pagaroutrasdespesasimposto": {
        "chave":    ["id"],
        "data":     "datavencimento",
        "decimais": ["valor", "basecalculo", "aliquota"],
    },
}


def transformar(tabela: str, spark, dbutils, ultima_particao: str = None):

    cfg = CONFIG[tabela]
    BRONZE     = f"varejinho.bronze.{tabela}"
    SILVER     = f"varejinho.silver.{tabela}"
    QUARENTENA = f"varejinho.silver._quarantine_{tabela}"
    CONTRACT   = f"{REPO}/contracts/silver/{tabela}.yaml"

    # 1. Leitura incremental
    df = spark.table(BRONZE)
    if ultima_particao:
        df = df.where(F.col("ingestion_date") > F.lit(ultima_particao))

    if df.isEmpty():
        print(f"[{tabela}] Nenhum dado novo. Encerrando.")
        return
    
    # 2. Schema drift
    detectar_drift(tabela, df, dbutils, spark)

    # 3. Cast de decimais brasileiros
    for col in cfg["decimais"]:
        if col in df.columns:
            df = df.withColumn(col,
                F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"))

    # 4. Cast de timestamp e derivação de ano/mes
    if cfg["data"] and cfg["data"] in df.columns:
        df = (df
            .withColumn(cfg["data"],
                F.to_timestamp(F.col(cfg["data"]), "yyyy/MM/dd HH:mm:ss.SSS"))
            .withColumn("ano", F.year(cfg["data"]))
            .withColumn("mes", F.month(cfg["data"])))
        
    # 4b. Cast de datas extras (nullable — usar try_to_timestamp)
    for col_extra in cfg.get("datas_extras", []):
        if col_extra in df.columns:
            df = df.withColumn(col_extra,
                F.expr(f"try_to_timestamp(`{col_extra}`, 'yyyy/MM/dd HH:mm:ss.SSS')"))


    # 5. Contrato — só se o arquivo existir
    try:
        validator = ContractValidator(CONTRACT)
        df_ok, df_quar, relatorio = validator.validate(df)
        print(f"[{tabela}] {relatorio}")
    except FileNotFoundError:
        print(f"[{tabela}] Contrato nao encontrado — seguindo sem validacao.")
        df_ok, df_quar, relatorio = df, df.where(F.lit(False)), {}

    # 6. Deduplicação pela chave real
    w = Window.partitionBy(*cfg["chave"]).orderBy(F.col("ingestion_date").desc())
    df_dedup = (
        df_ok
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 7. MERGE idempotente
    cond_merge = " AND ".join([f"t.{k} = s.{k}" for k in cfg["chave"]])

    if spark.catalog.tableExists(SILVER):
        (DeltaTable.forName(spark, SILVER).alias("t")
            .merge(df_dedup.alias("s"), cond_merge)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        writer = df_dedup.write.format("delta")
        if cfg["data"]:
            writer = writer.partitionBy("ano", "mes")
        writer.saveAsTable(SILVER)

    # 8. Quarentena
    if relatorio.get("quarentena", 0) > 0:
        (df_quar.write.format("delta")
            .mode("append")
            .saveAsTable(QUARENTENA))
        print(f"[{tabela}] {relatorio['quarentena']} registros em quarentena.")


# Rodar todas as tabelas em sequência
if __name__ == "__main__":
    for tabela in CONFIG:
        transformar(tabela, spark, dbutils)