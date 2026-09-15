-- Nenhuma venda deve ter valor_total = 0 com quantidade > 0
-- Indica erro de registro no PDV
select id_venda
from {{ source('gold', 'fato_vendas') }}
where valor_total = 0
  and quantidade > 0