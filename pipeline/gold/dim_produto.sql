-- pipeline/gold/dim_produto.sql
-- Dimensão de produto — SCD2 (uma linha por versão; SK = hash de id + valid_from)
-- Star schema: os nomes que o BI usa para filtrar ficam na própria dimensão.
--   • Hierarquia mercadológica achatada (seção, grupo, subgrupo): nome vindo da
--     árvore ATUAL (descrição é Type 1). O caminho de cada versão continua Type 2.
--     Medido em 25/09: 0 mudanças de estrutura na janela observada, 0 caminhos sem
--     nome e 0 caminhos duplicados. O Gold QG falha se um caminho deixar de ter nome.
--   • Tipo de embalagem e tipo de mercadoria: descrição no lugar do código.
-- A dim_mercadologico continua como referência da árvore (inclui subgrupos sem produto).

CREATE OR REPLACE TABLE varejinho.gold.dim_produto
USING DELTA
AS
WITH arvore AS (
    SELECT nivel, mercadologico1, mercadologico2, mercadologico3, trim(descricao) AS nome
    FROM varejinho.silver.mercadologico
    WHERE is_current
),
embalagem AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao FROM varejinho.silver.tipoembalagem
),
mercadoria AS (
    SELECT CAST(id AS INT) AS id, trim(descricao) AS descricao FROM varejinho.silver.tipomercadoria
)
SELECT
    md5(concat_ws('||', coalesce(CAST(p.id AS STRING), '<NULL>'), coalesce(CAST(p.valid_from AS STRING), '<NULL>'))) AS sk_produto,
    p.id                        AS id_produto,
    p.descricaocompleta         AS descricao_completa,
    p.descricaoreduzida         AS descricao_reduzida,
    p.ncm1                      AS ncm,
    te.descricao                AS tipo_embalagem,
    tm.descricao                AS tipo_mercadoria,
    p.pesoliquido,
    p.pesobruto,
    p.datacadastro,
    p.dataalteracao,

    -- Hierarquia: código (para ordenar e ligar à dim_mercadologico) + nome
    p.mercadologico1            AS secao,
    p.mercadologico2            AS grupo,
    p.mercadologico3            AS subgrupo,
    n1.nome                     AS secao_nome,
    n2.nome                     AS grupo_nome,
    n3.nome                     AS subgrupo_nome,

    p.valid_from,
    p.valid_to,
    p.is_current,
    p.hash_versao
FROM varejinho.silver.produto p
LEFT JOIN arvore n1
    ON  n1.nivel = '1' AND n1.mercadologico1 = p.mercadologico1
LEFT JOIN arvore n2
    ON  n2.nivel = '2' AND n2.mercadologico1 = p.mercadologico1
    AND n2.mercadologico2 = p.mercadologico2
LEFT JOIN arvore n3
    ON  n3.nivel = '3' AND n3.mercadologico1 = p.mercadologico1
    AND n3.mercadologico2 = p.mercadologico2 AND n3.mercadologico3 = p.mercadologico3
LEFT JOIN embalagem  te ON te.id = CAST(p.id_tipoembalagem  AS INT)
LEFT JOIN mercadoria tm ON tm.id = CAST(p.id_tipomercadoria AS INT)
ORDER BY p.id, p.valid_from
