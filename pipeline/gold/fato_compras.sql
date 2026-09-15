-- pipeline/gold/fato_compras.sql
-- Grão: 1 linha por item de pedido de compra
-- SK: MD5(id_pedidoitem || id_loja)
-- Join com dim_produto via id_produto (sem SCD2 temporal — pedido não tem data de venda)
-- Partição: ano/mes da data de compra do pedido

CREATE OR REPLACE TABLE varejinho.gold.fato_compras
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
    -- Surrogate key do fato
    md5(concat_ws('||', CAST(pi.id AS STRING), CAST(pi.id_loja AS STRING))) AS sk_compra,

    -- Chaves estrangeiras
    p.sk_produto,
    f.sk_fornecedor,
    pi.id_loja,
    CAST(date_format(pe.datacompra, 'yyyyMMdd') AS INT)                     AS sk_tempo,

    -- Chaves naturais
    pi.id                       AS id_pedidoitem,
    pi.id_pedido,

    -- Métricas do item
    pi.quantidade,
    pi.quantidadeatendida,
    pi.qtdembalagem,
    pi.custocompra,
    pi.custofinal,
    pi.valortotal,
    pi.desconto,
    pi.valorfrete,
    pi.valorrebaixa,
    pi.verbavalor,
    pi.custoverba,

    -- Atributos do cabeçalho do pedido
    pe.datacompra,
    pe.dataentrega,
    pe.id_situacaopedido,
    pe.id_tipoatendidopedido,

    -- Particionamento — herdado do pedido
    pe.ano,
    pe.mes

FROM varejinho.silver.pedidoitem pi

-- Join com cabeçalho do pedido
JOIN varejinho.silver.pedido pe
    ON pi.id_pedido = pe.id

-- Join com dim_produto — versão atual (pedido não tem data de referência confiável para SCD2)
LEFT JOIN varejinho.gold.dim_produto p
    ON  pi.id_produto = p.id_produto
    AND p.is_current  = true

-- Join com dim_fornecedor — versão atual
LEFT JOIN varejinho.gold.dim_fornecedor f
    ON  pe.id_fornecedor = f.id_fornecedor
    AND f.is_current     = true