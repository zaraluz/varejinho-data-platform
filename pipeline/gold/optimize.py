# Databricks notebook source
# pipeline/gold/optimize.py
# OPTIMIZE + ZORDER + VACUUM em todas as tabelas da Gold


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


def required_param(nome: str) -> str:
    """Parâmetro obrigatório do job: falha cedo em vez de cair num default de ambiente."""
    try:
        value = dbutils.widgets.get(nome)
    except Exception:
        value = ""
    if not value:
        raise ValueError(
            f"Parâmetro obrigatório ausente: '{nome}'. Execute via job do bundle, "
            "que injeta catalog/bundle_files_path/control_root/bronze_source_catalog por target."
        )
    return value


CATALOG = required_param("catalog")

tabelas_fato = [
    ("fato_vendas",            "sk_produto, id_loja"),
    ("fato_compras",           "sk_produto, sk_fornecedor"),
    ("fato_perdas",            "sk_produto"),
    ("fato_movimento_estoque", "sk_produto, id_loja"),
    ("fato_promocoes",         "sk_produto"),
    ("fato_oferta",            "sk_produto, id_loja"),
    ("fato_contas_pagar",      "sk_fornecedor"),
    ("fato_outras_despesas",   "sk_fornecedor"),
    ("fato_curva_abc",         "sk_produto"),
]

tabelas_dim = [
    "dim_produto", "dim_fornecedor", "dim_mercadologico", "dim_loja", "dim_tempo"
]

for tabela, zorder in tabelas_fato:
    spark.sql(f"OPTIMIZE {CATALOG}.gold.{tabela} ZORDER BY ({zorder})")
    print(f"✅ OPTIMIZE {CATALOG}.gold.{tabela}")

for tabela in tabelas_dim:
    spark.sql(f"OPTIMIZE {CATALOG}.gold.{tabela}")
    print(f"✅ OPTIMIZE {CATALOG}.gold.{tabela}")

# Mantém 30 dias de retenção para Time Travel.
todas = [t for t, _ in tabelas_fato] + tabelas_dim
for tabela in todas:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.gold.{tabela}
        SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
    """)
    spark.sql(f"VACUUM {CATALOG}.gold.{tabela} RETAIN 720 HOURS")
    print(f"✅ VACUUM {CATALOG}.gold.{tabela}")
