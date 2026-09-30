-- ops/storage/notaentrada_backfill_landing.sql
-- Landing imutável da carga histórica recuperada de notaentrada.
--
-- O arquivo foi recuperado de uma versão não corrente do S3 (a carga histórica
-- de 16/09 foi sobrescrita pela extração diária no mesmo caminho) e copiado para
-- um prefixo próprio, fora de bronze/:
--   s3://varejinho-lake/bronze_backfill/notaentrada/run_id=20260916T172331Z/notaentrada.csv
--
-- Por que fora de bronze/notaentrada/: lá ele viraria uma partição histórica nova,
-- e o mutation guard barra (com razão) qualquer partição committed que aparece ou
-- muda. Fora de bronze/ também fica fora da regra de lifecycle da Bronze.
-- Nome por run_id: cada carga ganha pasta própria, nada sobrescreve nada.
--
-- Quem executa: a dona do schema varejinho.bronze, no SQL editor (como o bootstrap
-- da Bronze). O service principal lê pela permissão que já tem no schema
-- (USE SCHEMA, SELECT). Idempotente.
-- Runbook: docs/runbooks/notaentrada_history_repair.md.

CREATE TABLE IF NOT EXISTS varejinho.bronze.notaentrada_backfill
USING CSV
OPTIONS (
  header = 'true',
  delimiter = ',',
  quote = '"',
  escape = '"',
  inferSchema = 'false',
  recursiveFileLookup = 'true'
)
LOCATION 's3://varejinho-lake/bronze_backfill/notaentrada/'
COMMENT 'Carga histórica recuperada de notaentrada (extração de 2026-09-16). Fonte única do reparo ops/repair/backfill_notaentrada_history.py.';

-- Conferência (os números esperados vêm do arquivo recuperado):
-- SELECT COUNT(*) AS linhas, COUNT(DISTINCT id) AS ids,
--        MIN(dataentrada) AS primeira, MAX(dataentrada) AS ultima
-- FROM varejinho.bronze.notaentrada_backfill;
