-- pipeline/gold/fato_promocoes.sql
-- Grão: 1 linha por produto em promoção
-- SK: MD5(id_promocaoitem || id_loja)
-- Join temporal com dim_produto pela data de início da promoção
-- Partição: ano/mes da data de início da promoção
-- Full load na Silver — inclui promoções futuras

CREATE OR REPLACE TABLE varejinho.gold.fato_promocoes
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', CAST(pi.id AS STRING), CAST(pr.id_loja AS STRING))) AS sk_promocao,

    -- Chaves estrangeiras
    p.sk_produto,
    pr.id_loja,
    CAST(date_format(pr.datainicio, 'yyyyMMdd') AS INT)                     AS sk_tempo,
    pr.id_tipopromocao,
    pr.id_situacaocadastro,

    -- Chaves naturais
    pi.id                       AS id_promocaoitem,
    pi.id_promocao,

    -- Métricas da promoção
    pi.precovenda               AS preco_promocional,
    pr.valor                    AS valor_promocao,
    pr.valordesconto,
    pr.quantidade               AS quantidade_minima,

    -- Atributos do cabeçalho
    pr.datainicio,
    pr.datatermino,
    pr.descricao                AS descricao_promocao,
    pr.aplicatodos,
    pr.somenteclubevantagens,

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