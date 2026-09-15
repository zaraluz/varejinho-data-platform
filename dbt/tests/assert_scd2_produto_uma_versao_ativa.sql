-- Cada produto deve ter exatamente 1 versão is_current = true
-- Garante que o MERGE SCD2 não deixou inconsistência
select id_produto, count(*) as versoes_ativas
from {{ source('gold', 'dim_produto') }}
where is_current = true
group by id_produto
having count(*) > 1