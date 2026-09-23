-- ops/bootstrap/grant_prod_service_principal.sql
-- Governança como código: privilégios mínimos do service principal que executa
-- os jobs de produção (run_as no target prod do bundle).
--
-- Principal: sp-varejinho-pipeline-prod
-- Application ID: bf71079b-64e1-4763-8b86-1e90718d8864
--
-- Quando executar: no cutover (F8/R7), DEPOIS do DEEP CLONE para `varejinho`
-- e ANTES do primeiro run de prod. Executar como owner/admin no SQL editor.
-- Idempotente: GRANT repetido não duplica privilégio.
--
-- Princípio: o SP lê a Bronze, escreve Silver/Gold/control e lê/escreve o
-- control storage no S3. Não recebe ALL PRIVILEGES, MANAGE nem CREATE SCHEMA.

-- 1) Entrar no catálogo
GRANT USE CATALOG ON CATALOG varejinho TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 2) Bronze: somente leitura (a Bronze pertence à ingestão do Pentaho)
GRANT USE SCHEMA, SELECT ON SCHEMA varejinho.bronze TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 3) Silver, Gold e control: ler, escrever (MERGE/UPDATE) e criar tabelas
--    (quarentena, CREATE OR REPLACE da Gold, tabelas de controle)
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA varejinho.silver  TO `bf71079b-64e1-4763-8b86-1e90718d8864`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA varejinho.gold    TO `bf71079b-64e1-4763-8b86-1e90718d8864`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA varejinho.control TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 4) Control storage no S3 (watermarks auxiliares, baselines de schema drift,
--    manifests de partição): leitura e escrita de arquivos.
--    <EXTERNAL_LOCATION>: confirmar o nome com `SHOW EXTERNAL LOCATIONS` e que a
--    URL cobre s3://varejinho-lake/_control antes de executar.
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `<EXTERNAL_LOCATION>` TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 5) Ownership das tabelas clonadas (Silver/Gold/control) é transferida para o SP
--    no runbook do cutover (ALTER TABLE ... OWNER TO), para que CREATE OR REPLACE
--    da Gold não dependa da conta pessoal (passo do runbook de cutover).

-- 6) Fora do SQL: SQL Warehouse "Serverless Starter Warehouse" -> Permissions ->
--    adicionar o SP com "Can use" (necessário para a task dbt).
