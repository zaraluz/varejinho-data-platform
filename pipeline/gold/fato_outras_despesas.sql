-- pipeline/gold/fato_outras_despesas.sql
-- Grão: 1 linha por despesa operacional
-- SK: MD5(id || id_loja)
-- Join temporal com dim_fornecedor pela data de emissão (quando houver fornecedor)
-- Partição: ano/mes da emissão
-- Situação por extenso; tipo de pagamento → dim_tipo_pagamento (conformada); tipo de entrada → dim_tipo_entrada

CREATE OR REPLACE TABLE varejinho.gold.fato_outras_despesas
USING DELTA
PARTITIONED BY (ano, mes)
AS
WITH situacao AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao
    FROM varejinho.silver.situacaopagaroutrasdespesas
)
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', coalesce(CAST(od.id AS STRING), '<NULL>'), coalesce(CAST(od.id_loja AS STRING), '<NULL>'))) AS sk_despesa,

    -- Chaves estrangeiras
    f.sk_fornecedor,
    od.id_loja                  AS sk_loja,
    CAST(date_format(od.dataemissao, 'yyyyMMdd') AS INT)                    AS sk_tempo,
    CAST(od.id_tipopagamento AS INT)                                        AS sk_tipo_pagamento,
    CAST(od.id_tipoentrada   AS INT)                                        AS sk_tipo_entrada,
    sd.descricao                AS situacao_despesa,

    -- Chave natural
    od.id                       AS id_despesa,
    od.numerodocumento,

    -- Métricas
    od.valor,
    od.valorbruto,

    -- Datas
    od.dataemissao,
    od.dataentrada,

    -- Particionamento
    od.ano,
    od.mes

FROM varejinho.silver.pagaroutrasdespesas od

LEFT JOIN situacao sd
    ON sd.id = CAST(od.id_situacaopagaroutrasdespesas AS INT)

-- Join temporal com dim_fornecedor pela emissão; permanece LEFT porque fornecedor é opcional
LEFT JOIN varejinho.gold.dim_fornecedor f
    ON  od.id_fornecedor = f.id_fornecedor
    AND od.dataemissao  >= f.valid_from
    AND (f.valid_to IS NULL OR od.dataemissao < f.valid_to)