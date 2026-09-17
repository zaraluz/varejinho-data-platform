# Databricks notebook source
# pipeline/bronze/ingest_bronze.py
# Bootstrap idempotente do ambiente.
# Em prod, garante as external tables da Bronze sobre o S3.
# Em dev, cria views sobre a Bronze de prod para evitar sobreposição de paths
# no Unity Catalog e manter o raw compartilhado somente para leitura.


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
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

# Catálogo de destino: Silver/Gold serão fisicamente isoladas por catálogo.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

sucesso = []
falha = []

for tabela in TABELAS:
    try:
        if CATALOG == BRONZE_SOURCE_CATALOG:
            # Prod: a Bronze é external e possui os arquivos raw no S3.
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
            objeto = "external table"
        else:
            # Dev: Unity Catalog não permite registrar outra table no mesmo path.
            # A view não duplica dados nem disputa ownership do caminho físico.
            spark.sql(f"""
                CREATE OR REPLACE VIEW {CATALOG}.bronze.{tabela}
                WITH SCHEMA EVOLUTION
                AS SELECT * FROM {BRONZE_SOURCE_CATALOG}.bronze.{tabela}
            """)
            objeto = f"view -> {BRONZE_SOURCE_CATALOG}.bronze.{tabela}"

        count = spark.table(f"{CATALOG}.bronze.{tabela}").count()
        sucesso.append(f"✅ {tabela}: {count:,} linhas ({objeto})")

    except Exception as e:
        falha.append(f"❌ {tabela}: {str(e)[:500]}")

print(
    f"\n=== BOOTSTRAP {CATALOG} | fonte Bronze: {BRONZE_SOURCE_CATALOG} | "
    f"{len(sucesso)} sucesso, {len(falha)} falha ===\n"
)
for s in sucesso:
    print(s)
for f in falha:
    print(f)

if falha:
    amostra = "\n".join(falha[:5])
    raise Exception(
        f"Bootstrap de {CATALOG} falhou em {len(falha)} objetos. "
        f"Primeiras falhas:\n{amostra}"
    )
