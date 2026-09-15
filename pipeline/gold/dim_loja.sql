CREATE OR REPLACE TABLE varejinho.gold.dim_loja
USING DELTA
AS
SELECT
    id                  AS sk_loja,
    id                  AS id_loja,
    descricao           AS nome_loja,
    id_regiao,
    lojavirtual,
    atacado
FROM varejinho.silver.loja
ORDER BY id