-- pipeline/gold/fato_contas_pagar.sql
-- Grão: 1 linha por parcela de pagamento a fornecedor
-- SK: MD5(id_parcela || id_loja)
-- Join temporal com dim_fornecedor pela data de emissão do documento
-- Partição: ano/mes do vencimento da parcela
-- Base para análise de DRE e fluxo de caixa
-- datapagamento nullable — parcelas não pagas retornam sk_tempo_pagamento NULL (2.362 casos)

CREATE OR REPLACE TABLE varejinho.gold.fato_contas_pagar
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    md5(concat_ws('||', CAST(pp.id AS STRING), CAST(pf.id_loja AS STRING))) AS sk_parcela,
    f.sk_fornecedor,
    pf.id_loja,
    CAST(date_format(pp.datavencimento, 'yyyyMMdd') AS INT)                 AS sk_tempo_vencimento,
    CAST(date_format(pp.datapagamento,  'yyyyMMdd') AS INT)                 AS sk_tempo_pagamento,
    pp.id_situacaopagarfornecedorparcela,
    pp.id_tipopagamento,
    pp.id                       AS id_parcela,
    pf.id                       AS id_pagarfornecedor,
    pp.numeroparcela,
    pf.numerodocumento,
    pp.valor,
    pp.valoracrescimo,
    pf.dataemissao,
    pf.dataentrada,
    pp.datavencimento,
    pp.datapagamento,
    pp.conferido,
    pp.ano,
    pp.mes
FROM varejinho.silver.pagarfornecedorparcela pp
JOIN varejinho.silver.pagarfornecedor pf
    ON pp.id_pagarfornecedor = pf.id
LEFT JOIN varejinho.gold.dim_fornecedor f
    ON  pf.id_fornecedor = f.id_fornecedor
    AND pf.dataemissao  >= f.valid_from
    AND (f.valid_to IS NULL OR pf.dataemissao < f.valid_to)