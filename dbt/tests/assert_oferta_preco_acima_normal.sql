{{ config(severity='warn') }}
-- Monitora ofertas em que o preço de oferta está acima do preço normal.
-- desconto_valor < 0 significa precooferta > preconormal.
-- É um sinal de anomalia comercial, não uma falha técnica do pipeline.
select id_oferta, sk_produto, sk_loja, desconto_valor
from {{ source('gold', 'fato_oferta') }}
where desconto_valor < 0
