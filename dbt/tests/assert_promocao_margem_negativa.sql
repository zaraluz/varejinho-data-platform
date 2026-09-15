{{ config(severity='warn') }}

-- Detecta promoções com preço abaixo do custo médio do produto
-- Mesmo padrão do assert_oferta_abaixo_do_custo — base para TCC
with custo_medio as (
    select
        sk_produto,
        avg(try_cast(replace(custocomimposto, ',', '.') as decimal(14,3))) as custo_medio
    from {{ source('gold', 'fato_vendas') }}
    where try_cast(replace(custocomimposto, ',', '.') as decimal(14,3)) > 0
    group by sk_produto
)
select
    p.id_promocaoitem,
    p.sk_produto,
    p.id_loja,
    p.preco_promocional,
    c.custo_medio,
    p.preco_promocional - c.custo_medio as margem_promocao
from {{ source('gold', 'fato_promocoes') }} p
join custo_medio c on p.sk_produto = c.sk_produto
where p.preco_promocional < c.custo_medio
  and p.preco_promocional > 0