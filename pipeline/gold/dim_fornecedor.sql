-- pipeline/gold/dim_fornecedor.sql
-- Dimensão fornecedor com histórico SCD2
-- SK por versão: MD5(id || valid_from)
-- CNPJ receberá column mask no Dia 6 (governança)

CREATE OR REPLACE TABLE varejinho.gold.dim_fornecedor
USING DELTA
AS
SELECT
    md5(concat_ws('||', coalesce(CAST(f.id AS STRING), '<NULL>'), coalesce(CAST(f.valid_from AS STRING), '<NULL>'))) AS sk_fornecedor,
    f.id                AS id_fornecedor,
    f.razaosocial       AS razao_social,
    f.nomefantasia      AS nome_fantasia,
    f.cnpj,
    f.valid_from,
    f.valid_to,
    f.is_current,
    f.hash_versao
FROM varejinho.silver.fornecedor f
ORDER BY f.id, f.valid_from