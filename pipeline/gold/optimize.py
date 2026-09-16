# Databricks notebook source
# pipeline/gold/optimize_vacuum.py
# OPTIMIZE + ZORDER + VACUUM em todas as tabelas da Gold
# Rodar após qualquer recriação de tabela ou semanalmente via DAB

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

# OPTIMIZE + ZORDER nos fatos
for tabela, zorder in tabelas_fato:
    spark.sql(f"OPTIMIZE varejinho.gold.{tabela} ZORDER BY ({zorder})")
    print(f"✅ OPTIMIZE {tabela}")

# OPTIMIZE nas dimensões (sem ZORDER — volume pequeno)
for tabela in tabelas_dim:
    spark.sql(f"OPTIMIZE varejinho.gold.{tabela}")
    print(f"✅ OPTIMIZE {tabela}")

# VACUUM em todas — retém 30 dias para Time Travel
todas = [t for t, _ in tabelas_fato] + tabelas_dim
for tabela in todas:
    spark.sql(f"""
        ALTER TABLE varejinho.gold.{tabela}
        SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
    """)
    spark.sql(f"VACUUM varejinho.gold.{tabela} RETAIN 720 HOURS")
    print(f"✅ VACUUM {tabela}")