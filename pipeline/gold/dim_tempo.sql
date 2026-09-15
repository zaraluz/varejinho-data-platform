-- pipeline/gold/dim_tempo.sql
-- Dimensão de tempo gerada por código — não extraída do ERP
-- Grão: 1 linha por dia | Intervalo: 2023-01-01 a 2027-12-31
-- SK: inteiro YYYYMMDD — legível e ordenável sem join

CREATE OR REPLACE TABLE varejinho.gold.dim_tempo
USING DELTA
AS
WITH datas AS (
  -- Gera uma sequência de inteiros e converte cada um em uma data
  SELECT
    date_add(DATE '2023-01-01', pos) AS data
  FROM (
    SELECT explode(sequence(0, datediff(DATE '2027-12-31', DATE '2023-01-01'))) AS pos
  )
)
SELECT
  -- Surrogate key: YYYYMMDD como inteiro (convenção de dim_tempo)
  CAST(date_format(data, 'yyyyMMdd') AS INT)  AS sk_tempo,

  -- Atributos de calendário
  data,
  YEAR(data)                                   AS ano,
  MONTH(data)                                  AS mes,
  DAYOFMONTH(data)                             AS dia,
  QUARTER(data)                                AS trimestre,
  WEEKOFYEAR(data)                             AS semana_ano,
  DAYOFWEEK(data)                              AS dia_semana_num,  -- 1=Dom, 7=Sáb

  -- Nomes legíveis (úteis no Power BI)
  date_format(data, 'MMMM')                   AS nome_mes,
  date_format(data, 'EEEE')                   AS nome_dia_semana,
  date_format(data, 'MMM/yyyy')               AS mes_ano,

  -- Flags booleanas
  CASE WHEN DAYOFWEEK(data) IN (1, 7)
       THEN TRUE ELSE FALSE END                AS is_fim_semana,

  CASE WHEN MONTH(data) IN (12, 1, 2)
       THEN 'Verão'
       WHEN MONTH(data) IN (3, 4, 5)
       THEN 'Outono'
       WHEN MONTH(data) IN (6, 7, 8)
       THEN 'Inverno'
       ELSE 'Primavera' END                    AS estacao_sul

FROM datas
ORDER BY data