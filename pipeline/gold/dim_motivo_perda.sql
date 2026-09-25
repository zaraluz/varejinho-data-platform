-- pipeline/gold/dim_motivo_perda.sql
-- Dimensão de motivo de perda — SCD1; SK = id do domínio. Usada por fato_perdas.
-- As contas contábeis do domínio ficam na Silver: não servem para cortar perdas.

CREATE OR REPLACE TABLE varejinho.gold.dim_motivo_perda
USING DELTA
AS
SELECT
    CAST(id AS INT)             AS sk_motivo_perda,
    CAST(id AS INT)             AS id_motivo_perda,
    trim(descricao)             AS motivo_perda,
    emitenota
FROM varejinho.silver.tipomotivoperda
ORDER BY id
