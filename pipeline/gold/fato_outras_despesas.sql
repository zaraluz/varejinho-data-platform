-- pipeline/gold/fato_outras_despesas.sql
-- Grão: 1 linha por despesa operacional
-- SK: MD5(id || id_loja)
-- Join temporal com dim_fornecedor pela data de emissão (quando houver fornecedor)
-- Partição: ano/mes da emissão

CREATE OR REPLACE TABLE varejinho.gold.fato_outras_despesas
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', CAST(od.id AS STRING), CAST(od.id_loja AS STRING))) AS sk_despesa,

    -- Chaves estrangeiras
    f.sk_fornecedor,
    od.id_loja,
    CAST(date_format(od.dataemissao, 'yyyyMMdd') AS INT)                    AS sk_tempo,
    od.id_situacaopagaroutrasdespesas,
    od.id_tipopagamento,
    od.id_tipoentrada,

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

-- Join temporal com dim_fornecedor pela emissão; permanece LEFT porque fornecedor é opcional
LEFT JOIN varejinho.gold.dim_fornecedor f
    ON  od.id_fornecedor = f.id_fornecedor
    AND od.dataemissao  >= f.valid_from
    AND (f.valid_to IS NULL OR od.dataemissao < f.valid_to)