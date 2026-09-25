-- pipeline/gold/dim_promocao.sql
-- Dimensão de promoção (cabeçalho) — SCD1, a Silver é full load; SK = id da promoção
-- (medido em 25/09: id é único, sem precisar da loja).
-- Os valores do cabeçalho (valor, desconto, quantidade mínima) são ATRIBUTOS aqui.
-- Antes ficavam em fato_promocoes, repetidos em cada item: somar no BI multiplicava
-- o valor da promoção pelo número de itens.

CREATE OR REPLACE TABLE varejinho.gold.dim_promocao
USING DELTA
AS
WITH tipo AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao FROM varejinho.silver.tipopromocao
),
situacao AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao FROM varejinho.silver.situacaocadastro
)
SELECT
    CAST(pr.id AS BIGINT)       AS sk_promocao,
    CAST(pr.id AS BIGINT)       AS id_promocao,
    pr.descricao                AS descricao_promocao,
    t.descricao                 AS tipo_promocao,
    s.descricao                 AS situacao_promocao,
    pr.datainicio,
    pr.datatermino,
    pr.valor                    AS valor_promocao,
    pr.valordesconto            AS valor_desconto,
    pr.quantidade               AS quantidade_minima,
    pr.aplicatodos,
    pr.somenteclubevantagens
FROM varejinho.silver.promocao pr
LEFT JOIN tipo     t ON t.id = CAST(pr.id_tipopromocao     AS INT)
LEFT JOIN situacao s ON s.id = CAST(pr.id_situacaocadastro AS INT)
ORDER BY pr.id
