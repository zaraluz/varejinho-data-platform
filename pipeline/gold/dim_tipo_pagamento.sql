-- pipeline/gold/dim_tipo_pagamento.sql
-- Dimensão CONFORMADA de tipo de pagamento — SCD1; SK = id do domínio.
-- Compartilhada por fato_contas_pagar e fato_outras_despesas: o mesmo filtro
-- corta os dois fatos com os mesmos rótulos.

CREATE OR REPLACE TABLE varejinho.gold.dim_tipo_pagamento
USING DELTA
AS
SELECT
    CAST(id AS INT)             AS sk_tipo_pagamento,
    CAST(id AS INT)             AS id_tipo_pagamento,
    trim(descricao)             AS tipo_pagamento,
    banco,
    cheque,
    boleto,
    docted,
    debitocc,
    quantidadedias
FROM varejinho.silver.tipopagamento
ORDER BY id
