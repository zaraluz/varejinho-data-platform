-- pipeline/gold/dim_tipo_entrada.sql
-- Dimensão de tipo de entrada — SCD1; SK = id do domínio. Usada por fato_outras_despesas.
-- A tabela do ERP tem ~50 colunas, quase todas de configuração contábil e fiscal;
-- ficam só os atributos que servem para analisar despesas. O resto continua na Silver.

CREATE OR REPLACE TABLE varejinho.gold.dim_tipo_entrada
USING DELTA
AS
SELECT
    CAST(id AS INT)             AS sk_tipo_entrada,
    CAST(id AS INT)             AS id_tipo_entrada,
    trim(descricao)             AS tipo_entrada,
    tipo,
    bonificacao,
    ativoimobilizado,
    foraestado,
    notaprodutor
FROM varejinho.silver.tipoentrada
ORDER BY id
