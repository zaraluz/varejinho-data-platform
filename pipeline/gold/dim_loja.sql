-- pipeline/gold/dim_loja.sql
-- Dimensão de loja/CD — SCD1; SK = id da loja.
-- id_regiao fica fora: tem um único valor e o domínio não é extraído do ERP.

CREATE OR REPLACE TABLE varejinho.gold.dim_loja
USING DELTA
AS
SELECT
    id                  AS sk_loja,
    id                  AS id_loja,
    descricao           AS nome_loja,
    lojavirtual,
    atacado
FROM varejinho.silver.loja
ORDER BY id