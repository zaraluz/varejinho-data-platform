-- pipeline/gold/fato_vendas.sql
-- Grão: 1 linha por item vendido por transação
-- SK: MD5(id || id_loja) — garante unicidade mesmo com múltiplas fontes
-- Join temporal com dim_produto SCD2 — pega a versão vigente na data da venda
-- Partição: ano/mes — baixa cardinalidade, Power BI sempre filtra período
-- Z-Order: sk_produto, sk_loja — alta cardinalidade, file skipping dentro da partição

CREATE OR REPLACE TABLE varejinho.gold.fato_vendas
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', CAST(v.id AS STRING), CAST(v.id_loja AS STRING))) AS sk_venda,

    -- Chaves estrangeiras para as dimensões
    p.sk_produto,
    v.id_loja                   AS sk_loja,
    CAST(date_format(v.data, 'yyyyMMdd') AS INT)  AS sk_tempo,

    -- Chave natural (para rastreabilidade)
    v.id                        AS id_venda,

    -- Métricas de venda
    v.quantidade,
    v.valor_total,
    v.precovenda,

    -- Métricas de custo e margem
    v.custocomimposto,
    v.custosemimposto,
    v.customediocomimposto,
    v.customediosemimposto,

    -- Métricas fiscais
    v.piscofins,
    v.piscofinscredito,
    v.icmscredito,
    v.icmsdebito,

    -- Flags
    v.oferta,
    v.perda,
    v.operacional,

    -- Particionamento
    v.ano,
    v.mes

FROM varejinho.silver.venda v

-- Join temporal com dim_produto — versão vigente na data da venda
LEFT JOIN varejinho.gold.dim_produto p
    ON  v.id_produto  = p.id_produto
    AND v.data        >= p.valid_from
    AND v.data        < COALESCE(p.valid_to, TIMESTAMP '2999-12-31 00:00:00')