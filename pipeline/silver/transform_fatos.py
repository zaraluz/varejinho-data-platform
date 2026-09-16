# pipeline/silver/transform_fatos.py
# Processa todas as tabelas fato Bronze → Silver
# Cast de tipos, schema drift, contrato, quarentena e MERGE idempotente
# Código inline — imports instáveis em serverless

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json

REPO     = "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform"
REGISTRY = "s3://varejinho-lake/_control/schema_registry"

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
        "try_decimais": ["precoimediato"],
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

# ── Schema drift (inline) ────────────────────────────────────────────────────
def detectar_drift(tabela, df, dbutils):
    schema_atual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    registry_file = f"{REGISTRY}/{tabela}.json"
    try:
        conteudo = dbutils.fs.head(registry_file)
        schema_anterior = json.loads(conteudo)
    except Exception:
        dbutils.fs.put(registry_file, json.dumps(schema_atual), overwrite=True)
        print(f"[{tabela}] Schema baseline criado.")
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

# ── Contract validator (inline) ──────────────────────────────────────────────
def validar_contrato(tabela, df, contract_path):
    import yaml
    try:
        with open(contract_path, "r") as f:
            contract = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[{tabela}] Contrato não encontrado — seguindo sem validação.")
        return df, df.where(F.lit(False)), {}

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
        "tabela":     tabela,
        "total":      total,
        "validos":    total - invalidos,
        "quarentena": invalidos,
    }
    return df_ok, df_quar, relatorio

# ── Pipeline por tabela ──────────────────────────────────────────────────────
def transformar(tabela: str, spark, dbutils, ultima_particao: str = None):
    cfg        = CONFIG[tabela]
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

    # 2. Cast de decimais
    for col in cfg["decimais"]:
        if col in df.columns:
            df = df.withColumn(col,
                F.regexp_replace(F.col(col), ",", ".").cast("decimal(14,3)"))

    # 2b. Cast tolerante — colunas com valores não numéricos (ex: 'N')
    for col in cfg.get("try_decimais", []):
        if col in df.columns:
            df = df.withColumn(col,
                F.expr(f"try_cast(replace(`{col}`, ',', '.') as decimal(14,3))"))

    # 3. Cast de timestamp principal e derivação de ano/mes
    if cfg["data"] and cfg["data"] in df.columns:
        df = (df
            .withColumn(cfg["data"],
                F.to_timestamp(F.col(cfg["data"]), "yyyy/MM/dd HH:mm:ss.SSS"))
            .withColumn("ano", F.year(cfg["data"]))
            .withColumn("mes", F.month(cfg["data"])))

    # 4. Cast de datas extras (nullable)
    for col_extra in cfg.get("datas_extras", []):
        if col_extra in df.columns:
            df = df.withColumn(col_extra,
                F.expr(f"try_to_timestamp(`{col_extra}`, 'yyyy/MM/dd HH:mm:ss.SSS')"))

    # 5. Schema drift — após cast, compara schema Silver com baseline
    detectar_drift(tabela, df, dbutils)

    # 6. Contrato
    df_ok, df_quar, relatorio = validar_contrato(tabela, df, CONTRACT)
    if relatorio:
        print(f"[{tabela}] {relatorio}")

    # 7. Deduplicação
    w = Window.partitionBy(*cfg["chave"]).orderBy(F.col("ingestion_date").desc())
    df_dedup = (df_ok
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn"))

    # 8. MERGE idempotente
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

    # 9. Quarentena
    if relatorio.get("quarentena", 0) > 0:
        (df_quar.write.format("delta")
            .mode("append")
            .saveAsTable(QUARENTENA))
        print(f"[{tabela}] {relatorio['quarentena']} registros em quarentena.")

    count = spark.table(SILVER).count()
    print(f"✅ {tabela}: {count:,} linhas na Silver")

# ── Execução ─────────────────────────────────────────────────────────────────
for tabela in CONFIG:
    transformar(tabela, spark, dbutils)