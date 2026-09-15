{{ config(severity='warn') }}
-- Detecta ofertas com preço acima do preço normal
-- desconto_valor < 0 significa precooferta > preconormal
-- Resultado esperado: 9.198 registros (monitorar variação)
select id_oferta, sk_produto, id_loja, desconto_valor
from {{ source('gold', 'fato_oferta') }}
where desconto_valor < 0