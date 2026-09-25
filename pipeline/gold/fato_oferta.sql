-- pipeline/gold/fato_oferta.sql
-- Grão: 1 linha por produto em oferta por loja
-- SK: MD5(id || id_loja)
-- Join com dim_produto — versão vigente na data de início da oferta
-- Partição: ano/mes da data de início da oferta
-- Tipo de oferta → dim_tipo_oferta; id_situacaooferta fica fora (domínio não extraído)
-- Full load na Silver — inclui ofertas futuras
-- Base para TCC: detecção de margem negativa em oferta

CREATE OR REPLACE TABLE varejinho.gold.fato_oferta
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', coalesce(CAST(o.id AS STRING), '<NULL>'), coalesce(CAST(o.id_loja AS STRING), '<NULL>')))   AS sk_oferta,

    -- Chaves estrangeiras
    p.sk_produto,
    o.id_loja                   AS sk_loja,
    CAST(date_format(o.datainicio, 'yyyyMMdd') AS INT)                      AS sk_tempo,
    CAST(o.id_tipooferta AS INT)                                            AS sk_tipo_oferta,

    -- Chave natural
    o.id                        AS id_oferta,

    -- Métricas de preço
    o.precooferta,
    o.preconormal,
    o.preconormal - o.precooferta                                           AS desconto_valor,
    CASE WHEN o.preconormal > 0
         THEN ROUND((o.preconormal - o.precooferta) / o.preconormal * 100, 2)
         ELSE NULL END                                                       AS desconto_percentual,

    -- Flags
    o.ofertafamilia,
    o.ofertaassociado,
    o.bloquearvenda,
    o.cashback,
    o.encerraoferta,

    -- Período da oferta
    o.datainicio,
    o.datatermino,

    -- Particionamento
    o.ano,
    o.mes

FROM varejinho.silver.oferta o

-- Join temporal com dim_produto — versão vigente na data de início da oferta
LEFT JOIN varejinho.gold.dim_produto p
    ON  o.id_produto  = p.id_produto
    AND o.datainicio  >= p.valid_from
    AND o.datainicio  < COALESCE(p.valid_to, TIMESTAMP '2999-12-31 00:00:00')