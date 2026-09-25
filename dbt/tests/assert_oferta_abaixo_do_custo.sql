{{ config(severity='warn') }}
-- Detecta ofertas com preço abaixo do custo médio do produto
-- Detecta ofertas com preço abaixo do custo médio do produto
-- Cruza fato_oferta com o custo médio de compra do produto em fato_vendas
-- Resultado > 0 indica margem negativa real — base para o TCC
with custo_medio AS (
    select
        sk_produto,
        avg(try_cast(replace(custocomimposto, ',', '.') as decimal(14,3))) as custo_medio
    from {{ source('gold', 'fato_vendas') }}
    where try_cast(replace(custocomimposto, ',', '.') as decimal(14,3)) > 0
    group by sk_produto
)
select
    o.id_oferta,
    o.sk_produto,
    o.sk_loja,
    o.precooferta,
    o.preconormal,
    c.custo_medio,
    o.precooferta - c.custo_medio as margem_oferta
from {{ source('gold', 'fato_oferta') }} o
join custo_medio c on o.sk_produto = c.sk_produto
where o.precooferta < c.custo_medio
  and o.precooferta > 0