-- ops/bootstrap/grant_prod_service_principal.sql
-- Governança como código: privilégios mínimos do service principal que executa
-- os jobs de produção (run_as no target prod do bundle).
--
-- Principal: sp-varejinho-pipeline-prod
-- Application ID: bf71079b-64e1-4763-8b86-1e90718d8864
--
-- Quando executar: no cutover, DEPOIS do step=clone (que cria varejinho.control)
-- e ANTES do step=ownership e do primeiro run de prod. Executar como owner/admin
-- no SQL editor. Runbook: docs/runbooks/production_cutover.md.
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
--    `varejinho_lake_new` = s3://varejinho-lake/ (confirmado com SHOW EXTERNAL
--    LOCATIONS em 24/09). A `varejinho_lake` aponta para o bucket antigo e não é usada.
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `varejinho_lake_new` TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 5) Ownership das tabelas clonadas (Silver/Gold/control) é transferida para o SP
--    no runbook do cutover (ALTER TABLE ... OWNER TO), para que CREATE OR REPLACE
--    da Gold não dependa da conta pessoal (passo do runbook de cutover).

-- 6) Fora do SQL: SQL Warehouse "Serverless Starter Warehouse" -> Permissions ->
--    adicionar o SP com "Can use" (necessário para a task dbt).
