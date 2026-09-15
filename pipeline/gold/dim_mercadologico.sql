-- pipeline/gold/dim_mercadologico.sql
-- Hierarquia mercadológica — seção / grupo / subgrupo
-- SCD2 — reorganizações de categoria precisam de histórico

CREATE OR REPLACE TABLE varejinho.gold.dim_mercadologico
USING DELTA
AS
SELECT
    md5(concat_ws('||', CAST(id AS STRING), CAST(valid_from AS STRING))) AS sk_mercadologico,
    id                  AS id_mercadologico,
    descricao,
    mercadologico1      AS secao,
    mercadologico2      AS grupo,
    mercadologico3      AS subgrupo,
    nivel,
    valid_from,
    valid_to,
    is_current,
    hash_versao
FROM varejinho.silver.mercadologico
ORDER BY id, valid_from