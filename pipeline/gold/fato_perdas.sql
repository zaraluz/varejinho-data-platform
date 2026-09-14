-- pipeline/gold/fato_perdas.sql
-- Grão: 1 linha por registro de perda
-- SK: MD5(id || id_loja)
-- Join temporal com dim_produto SCD2 — versão vigente na data da perda
-- Partição: ano/mes da data da perda

CREATE OR REPLACE TABLE varejinho.gold.fato_perdas
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', CAST(pe.id AS STRING), CAST(pe.id_loja AS STRING))) AS sk_perda,

    -- Chaves estrangeiras
    p.sk_produto,
    pe.id_loja,
    CAST(date_format(pe.data, 'yyyyMMdd') AS INT)                           AS sk_tempo,
    pe.id_tipomotivoperda,

    -- Chave natural
    pe.id                       AS id_perda,

    -- Métricas
    pe.quantidade,
    pe.custocomimposto,
    pe.custosemimposto,
    pe.customediocomimposto,
    pe.customediosemimposto,
    pe.valorpis,
    pe.valorcofins,
    pe.valoripi,
    pe.valoricmssubstituicao,
    pe.valorbasepiscofins,

    -- Particionamento
    pe.ano,
    pe.mes

FROM varejinho.silver.perda pe

-- Join temporal com dim_produto — versão vigente na data da perda
LEFT JOIN varejinho.gold.dim_produto p
    ON  pe.id_produto = p.id_produto
    AND pe.data       >= p.valid_from
    AND pe.data       < COALESCE(p.valid_to, TIMESTAMP '2999-12-31 00:00:00')