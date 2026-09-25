-- pipeline/gold/fato_contas_pagar.sql
-- Grão: 1 linha por parcela de pagamento a fornecedor
-- SK: MD5(id_parcela || id_loja)
-- Join temporal com dim_fornecedor pela data de emissão do documento
-- Partição: ano/mes do vencimento da parcela
-- Base para análise de DRE e fluxo de caixa
-- Situação da parcela por extenso; tipo de pagamento → dim_tipo_pagamento (conformada)
-- datapagamento nullable — parcelas não pagas retornam sk_tempo_pagamento NULL

CREATE OR REPLACE TABLE varejinho.gold.fato_contas_pagar
USING DELTA
PARTITIONED BY (ano, mes)
AS
WITH situacao AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao
    FROM varejinho.silver.situacaopagarfornecedorparcela
)
SELECT
    md5(concat_ws('||', CAST(pp.id AS STRING), CAST(pf.id_loja AS STRING))) AS sk_parcela,
    f.sk_fornecedor,
    pf.id_loja                  AS sk_loja,
    CAST(date_format(pp.datavencimento, 'yyyyMMdd') AS INT)                 AS sk_tempo_vencimento,
    CAST(date_format(pp.datapagamento,  'yyyyMMdd') AS INT)                 AS sk_tempo_pagamento,
    CAST(pp.id_tipopagamento AS INT)                                        AS sk_tipo_pagamento,
    sp.descricao                AS situacao_parcela,
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
LEFT JOIN situacao sp
    ON sp.id = CAST(pp.id_situacaopagarfornecedorparcela AS INT)
LEFT JOIN varejinho.gold.dim_fornecedor f
    ON  pf.id_fornecedor = f.id_fornecedor
    AND pf.dataemissao  >= f.valid_from
    AND (f.valid_to IS NULL OR pf.dataemissao < f.valid_to)