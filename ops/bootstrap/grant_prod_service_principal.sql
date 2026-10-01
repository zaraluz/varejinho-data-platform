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
-- Princípio: o SP lê a Bronze (só como tabela), escreve Silver/Gold/control e
-- lê/escreve o volume de controle. Nenhum privilégio de arquivo no bucket.
-- Não recebe ALL PRIVILEGES, MANAGE nem CREATE SCHEMA.
-- Pessoas só leem prod, por um grupo (seção 7): quem escreve é o SP.

-- 1) Entrar no catálogo
GRANT USE CATALOG ON CATALOG varejinho TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 2) Bronze: somente leitura (a Bronze pertence à ingestão do Pentaho)
GRANT USE SCHEMA, SELECT ON SCHEMA varejinho.bronze TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 3) Silver, Gold e control: ler, escrever (MERGE/UPDATE) e criar tabelas
--    (quarentena, CREATE OR REPLACE da Gold, tabelas de controle)
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA varejinho.silver  TO `bf71079b-64e1-4763-8b86-1e90718d8864`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA varejinho.gold    TO `bf71079b-64e1-4763-8b86-1e90718d8864`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA varejinho.control TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 4) Control storage (baselines de schema drift, manifests de partição): o volume
--    externo varejinho.control.control_files = s3://varejinho-lake/_control/, criado
--    por ops/storage/control_volumes.sql (criar o volume antes deste GRANT).
--    Nenhum privilégio na external location `varejinho_lake_new` = s3://varejinho-lake/,
--    que inclui bronze/: a Bronze é lida só como tabela (seção 2).
--    Histórico: até o lote pós-F9 o SP tinha READ FILES, WRITE FILES na external
--    location inteira; a revogação e o rollback estão em ops/storage/control_volumes.sql.
GRANT READ VOLUME, WRITE VOLUME ON VOLUME varejinho.control.control_files TO `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- 5) Ownership das tabelas clonadas (Silver/Gold/control) é transferida para o SP
--    no runbook do cutover (ALTER TABLE ... OWNER TO), para que CREATE OR REPLACE
--    da Gold não dependa da conta pessoal (passo do runbook de cutover).

-- 6) Fora do SQL: SQL Warehouse "Serverless Starter Warehouse" -> Permissions ->
--    adicionar o SP com "Can use" (necessário para a task dbt).

-- 7) Leitura humana em prod: grupo, não pessoa (RBAC). Depois do step=ownership
--    as tabelas pertencem ao SP; ser dona do catálogo e dos schemas permite
--    conceder privilégios nelas, mas não dá SELECT. O grupo só lê.
--    Criar o grupo antes (membros: operadores; entitlement: só Consumer access)
--    e aplicar antes do step=ownership, que confere a leitura no final.
GRANT USE CATALOG ON CATALOG varejinho TO `varejinho-prod-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA varejinho.bronze  TO `varejinho-prod-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA varejinho.silver  TO `varejinho-prod-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA varejinho.gold    TO `varejinho-prod-readers`;
GRANT USE SCHEMA, SELECT ON SCHEMA varejinho.control TO `varejinho-prod-readers`;
GRANT READ VOLUME ON VOLUME varejinho.control.control_files TO `varejinho-prod-readers`;

-- 8) Fora do SQL: quem faz o `bundle deploy -t prod` precisa do papel
--    "Service Principal: User" no SP (Settings -> Identity and access ->
--    Service principals -> Permissions). Sem ele, a API de Jobs recusa o run_as
--    com 403; o `bundle validate` não confere autorização.
