-- Toda venda deve ter sk_tempo dentro do intervalo da dim_tempo
-- sk_tempo fora do intervalo indica venda com data fora de 2023-2027
select v.id_venda, v.sk_tempo
from {{ source('gold', 'fato_vendas') }} v
left join {{ source('gold', 'dim_tempo') }} t on v.sk_tempo = t.sk_tempo
where t.sk_tempo is null