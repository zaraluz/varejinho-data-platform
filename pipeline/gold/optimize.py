# Databricks notebook source
# pipeline/gold/optimize.py
# OPTIMIZE + ZORDER + VACUUM em todas as tabelas da Gold


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")

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
