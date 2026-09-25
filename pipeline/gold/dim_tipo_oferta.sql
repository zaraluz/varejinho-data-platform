-- pipeline/gold/dim_tipo_oferta.sql
-- Dimensão de tipo de oferta — SCD1; SK = id do domínio. Usada por fato_oferta.

CREATE OR REPLACE TABLE varejinho.gold.dim_tipo_oferta
USING DELTA
AS
SELECT
    CAST(id AS INT)             AS sk_tipo_oferta,
    CAST(id AS INT)             AS id_tipo_oferta,
    trim(descricao)             AS tipo_oferta,
    prioridade,
    desconsiderarofertavendamedia,
    scanntech
FROM varejinho.silver.tipooferta
ORDER BY id
