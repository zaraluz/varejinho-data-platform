# Databricks notebook source
# pipeline/bronze/ingest_bronze.py
# Cria catálogo/schemas do ambiente e registra as 37 tabelas Bronze como external tables.
# Bronze é compartilhada fisicamente no S3; Silver/Gold ficam isoladas por catálogo.


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BASE_PATH = "s3://varejinho-lake/bronze"

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

# Bootstrap idempotente do ambiente.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

sucesso = []
falha = []

for tabela in TABELAS:
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.{tabela}
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
        count = spark.table(f"{CATALOG}.bronze.{tabela}").count()
        sucesso.append(f"✅ {tabela}: {count:,} linhas")

    except Exception as e:
        falha.append(f"❌ {tabela}: {str(e)[:160]}")

print(f"\n=== BOOTSTRAP {CATALOG}: {len(sucesso)} sucesso, {len(falha)} falha ===\n")
for s in sucesso:
    print(s)
for f in falha:
    print(f)

if falha:
    raise Exception(f"Bootstrap de {CATALOG} falhou em {len(falha)} tabelas")
