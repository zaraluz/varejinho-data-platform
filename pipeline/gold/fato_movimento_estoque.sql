-- pipeline/gold/fato_movimento_estoque.sql
-- Grão: 1 linha por movimentação de estoque
-- SK: MD5(id || id_loja)
-- Join temporal com dim_produto SCD2 — versão vigente na data da movimentação
-- Partição: ano/mes da data da movimentação
-- Cobre lojas e CD — diferente da tabela estoque que não cobria o CD
-- Decimais castados na Gold — logestoque chegou como string da Silver

CREATE OR REPLACE TABLE varejinho.gold.fato_movimento_estoque
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    md5(concat_ws('||', CAST(le.id AS STRING), CAST(le.id_loja AS STRING))) AS sk_movimento,
    p.sk_produto,
    le.id_loja,
    CAST(date_format(le.datamovimento, 'yyyyMMdd') AS INT)                  AS sk_tempo,
    le.id_tipomovimentacao,
    le.id_tipoentradasaida,
    le.id                                                                    AS id_movimento,
    le.id_venda,
    CAST(REPLACE(le.quantidade,           ',', '.') AS DECIMAL(14,3))       AS quantidade,
    CAST(REPLACE(le.estoqueanterior,      ',', '.') AS DECIMAL(14,3))       AS estoqueanterior,
    CAST(REPLACE(le.estoqueatual,         ',', '.') AS DECIMAL(14,3))       AS estoqueatual,
    CAST(REPLACE(le.estoqueatual,         ',', '.') AS DECIMAL(14,3)) -
    CAST(REPLACE(le.estoqueanterior,      ',', '.') AS DECIMAL(14,3))       AS variacao_estoque,
    CAST(REPLACE(le.custocomimposto,      ',', '.') AS DECIMAL(14,3))       AS custocomimposto,
    CAST(REPLACE(le.custosemimposto,      ',', '.') AS DECIMAL(14,3))       AS custosemimposto,
    CAST(REPLACE(le.customediocomimposto, ',', '.') AS DECIMAL(14,3))       AS customediocomimposto,
    CAST(REPLACE(le.customediosemimposto, ',', '.') AS DECIMAL(14,3))       AS customediosemimposto,
    le.ano,
    le.mes
FROM varejinho.silver.logestoque le
LEFT JOIN varejinho.gold.dim_produto p
    ON  le.id_produto        = p.id_produto
    AND le.datamovimento     >= p.valid_from
    AND le.datamovimento      < COALESCE(p.valid_to, TIMESTAMP '2999-12-31 00:00:00')