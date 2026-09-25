-- pipeline/gold/fato_promocoes.sql
-- Grão: 1 linha por produto em promoção (item)
-- SK do fato: sk_promocao_item = MD5(id_promocaoitem || id_loja)
-- Cabeçalho da promoção (descrição, tipo, situação, datas, valor, desconto, quantidade
-- mínima) fica em dim_promocao: aqui ele se repetiria em cada item e somaria errado.
-- Join temporal com dim_produto pela data de início da promoção
-- Partição: ano/mes da data de início da promoção
-- Full load na Silver — inclui promoções futuras

CREATE OR REPLACE TABLE varejinho.gold.fato_promocoes
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', CAST(pi.id AS STRING), CAST(pr.id_loja AS STRING))) AS sk_promocao_item,

    -- Chaves estrangeiras
    CAST(pi.id_promocao AS BIGINT)                                          AS sk_promocao,
    p.sk_produto,
    pr.id_loja                  AS sk_loja,
    CAST(date_format(pr.datainicio, 'yyyyMMdd') AS INT)                     AS sk_tempo,

    -- Chaves naturais
    pi.id                       AS id_promocaoitem,

    -- Métrica do item
    pi.precovenda               AS preco_promocional,

    -- Particionamento — herdado do cabeçalho
    pr.ano,
    pr.mes

FROM varejinho.silver.promocaoitem pi

-- Join com cabeçalho da promoção
JOIN varejinho.silver.promocao pr
    ON pi.id_promocao = pr.id

-- Join temporal com dim_produto — versão vigente no início da promoção
LEFT JOIN varejinho.gold.dim_produto p
    ON  pi.id_produto  = p.id_produto
    AND pr.datainicio >= p.valid_from
    AND (p.valid_to IS NULL OR pr.datainicio < p.valid_to)