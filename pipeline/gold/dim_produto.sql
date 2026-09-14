CREATE OR REPLACE TABLE varejinho.gold.dim_produto
USING DELTA
AS
SELECT
    md5(concat_ws('||', CAST(p.id AS STRING), CAST(p.valid_from AS STRING))) AS sk_produto,
    p.id                        AS id_produto,
    p.descricaocompleta         AS descricao_completa,
    p.descricaoreduzida         AS descricao_reduzida,
    p.ncm1                      AS ncm,
    p.id_tipoembalagem,
    p.id_tipomercadoria,
    p.pesoliquido,
    p.pesobruto,
    p.datacadastro,
    p.dataalteracao,
    p.mercadologico1            AS secao,
    p.mercadologico2            AS grupo,
    p.mercadologico3            AS subgrupo,
    p.valid_from,
    p.valid_to,
    p.is_current,
    p.hash_versao
FROM varejinho.silver.produto p
ORDER BY p.id, p.valid_from