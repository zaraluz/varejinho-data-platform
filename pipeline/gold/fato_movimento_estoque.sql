-- pipeline/gold/fato_movimento_estoque.sql
-- Grão: 1 linha por movimentação de estoque
-- SK: MD5(id || id_loja)
-- Join temporal com dim_produto SCD2 — versão vigente na data da movimentação
-- Partição: ano/mes da data da movimentação
-- Tipo de movimentação por extenso; id_tipoentradasaida fica fora (domínio não extraído)
-- Cobre lojas e CD — diferente da tabela estoque que não cobria o CD

CREATE OR REPLACE TABLE varejinho.gold.fato_movimento_estoque
USING DELTA
PARTITIONED BY (ano, mes)
AS
WITH tipo AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao FROM varejinho.silver.tipomovimentacao
)
SELECT
    md5(concat_ws('||', CAST(le.id AS STRING), CAST(le.id_loja AS STRING))) AS sk_movimento,
    p.sk_produto,
    le.id_loja                  AS sk_loja,
    CAST(date_format(le.datamovimento, 'yyyyMMdd') AS INT)                  AS sk_tempo,
    tm.descricao                AS tipo_movimentacao,
    le.id                                                                    AS id_movimento,
    le.id_venda,
    le.quantidade,
    le.estoqueanterior,
    le.estoqueatual,
    le.estoqueatual - le.estoqueanterior                                     AS variacao_estoque,
    le.custocomimposto,
    le.custosemimposto,
    le.customediocomimposto,
    le.customediosemimposto,
    le.ano,
    le.mes
FROM varejinho.silver.logestoque le
LEFT JOIN tipo tm
    ON tm.id = CAST(le.id_tipomovimentacao AS INT)
LEFT JOIN varejinho.gold.dim_produto p
    ON  le.id_produto        = p.id_produto
    AND le.datamovimento     >= p.valid_from
    AND le.datamovimento      < COALESCE(p.valid_to, TIMESTAMP '2999-12-31 00:00:00')