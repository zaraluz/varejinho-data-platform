-- pipeline/gold/fato_curva_abc.sql
-- Grão: 1 linha por produto/loja/snapshot_date
-- SK: MD5(id_produto || id_loja || snapshot_date)
-- Sem join temporal — snapshot já é o estado naquela data
-- Partição: snapshot_date
-- Permite analisar migração de curva: "quais produtos saíram da curva A no trimestre?"

CREATE OR REPLACE TABLE varejinho.gold.fato_curva_abc
USING DELTA
PARTITIONED BY (snapshot_date)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||',
        CAST(c.id_produto AS STRING),
        CAST(c.id_loja AS STRING),
        CAST(c.snapshot_date AS STRING)))                                    AS sk_curva,

    -- Chaves estrangeiras
    p.sk_produto,
    c.id_loja,
    CAST(date_format(c.snapshot_date, 'yyyyMMdd') AS INT)                   AS sk_tempo,

    -- Chave natural
    c.id_produto,

    -- Classificação geral (2 níveis)
    c.id_tipocurvaabc_nivel1                                                AS curva_geral_nivel1,
    c.id_tipocurvaabc_nivel2                                                AS curva_geral_nivel2,

    -- Classificação por nível mercadológico
    c.id_tipocurvaabcmercadologico1_nivel1                                  AS curva_secao_nivel1,
    c.id_tipocurvaabcmercadologico1_nivel2                                  AS curva_secao_nivel2,
    c.id_tipocurvaabcmercadologico2_nivel1                                  AS curva_grupo_nivel1,
    c.id_tipocurvaabcmercadologico2_nivel2                                  AS curva_grupo_nivel2,
    c.id_tipocurvaabcmercadologico3_nivel1                                  AS curva_subgrupo_nivel1,
    c.id_tipocurvaabcmercadologico3_nivel2                                  AS curva_subgrupo_nivel2,

    -- Métricas do período do snapshot
    c.quantidade,
    c.valortotal,
    c.lucro,

    -- Data do snapshot
    c.snapshot_date

FROM varejinho.silver.curvaabc c

-- Join com dim_produto — versão vigente na data do snapshot
LEFT JOIN varejinho.gold.dim_produto p
    ON  c.id_produto      = p.id_produto
    AND c.snapshot_date   >= p.valid_from
    AND c.snapshot_date    < COALESCE(p.valid_to, TIMESTAMP '2999-12-31 00:00:00')