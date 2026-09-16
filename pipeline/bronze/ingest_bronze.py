# Databricks notebook source
# pipeline/bronze/registro_bronze.py
# Registra as 37 tabelas da Bronze como external tables no Unity Catalog
# Rodar apenas uma vez na configuração inicial ou após recriar o catálogo
#
# Pré-requisitos:
# - Catálogo varejinho criado com schemas bronze, silver e gold
# - External Location varejinho_lake_new apontando para s3://varejinho-lake/
# - Arquivos CSV presentes em s3://varejinho-lake/bronze/<tabela>/ingestion_date=<data>/
#
# Observação:
# Pentaho Community Edition não tem Parquet Output nativo — Bronze é CSV.
# Conversão para Delta acontece na Silver.

TABELAS = [
    "curvaabc", "fornecedor", "logestoque", "loja", "mercadologico",
    "notaentrada", "notaentradaitem", "oferta", "pagarfornecedor",
    "pagarfornecedorparcela", "pagaroutrasdespesas", "pagaroutrasdespesasimposto",
    "pedido", "pedidoitem", "perda", "produto", "produtofornecedor",
    "promocao", "promocaoitem", "situacaocadastro", "situacaonotaentrada",
    "situacaopagarfornecedorparcela", "situacaopagaroutrasdespesas",
    "situacaopedido", "tipocurvaabc", "tipoembalagem", "tipoentrada",
    "tipofornecedor", "tipomercadoria", "tipomotivoperda", "tipomovimentacao",
    "tipooferta", "tipopagamento", "tipopedido", "tipoplanoconta",
    "tipopromocao", "venda",
]

BASE_PATH = "s3://varejinho-lake/bronze"
sucesso = []
falha = []

for tabela in TABELAS:
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS varejinho.bronze.{tabela}
            USING CSV
            OPTIONS (
                header = 'true',
                delimiter = ',',
                quote = '"',
                escape = '"',
                inferSchema = 'false',
                recursiveFileLookup = 'true'
            )
            LOCATION '{BASE_PATH}/{tabela}/'
        """)
        count = spark.table(f"varejinho.bronze.{tabela}").count()
        sucesso.append(f"✅ {tabela}: {count:,} linhas")

    except Exception as e:
        falha.append(f"❌ {tabela}: {str(e)[:100]}")

print(f"\n=== RESULTADO: {len(sucesso)} sucesso, {len(falha)} falha ===\n")
for s in sucesso: print(s)
for f in falha: print(f)
