-- ops/storage/control_volumes.sql
-- Control storage como volume externo do Unity Catalog, um por ambiente.
-- Executar como owner/admin no SQL editor, cada bloco no seu momento do runbook:
-- docs/runbooks/control_volume_migration.md.
--
-- Por que: o service principal de prod tinha READ FILES + WRITE FILES na external
-- location `varejinho_lake_new` = s3://varejinho-lake/ inteiro, que inclui bronze/
-- (o dado bruto, única cópia fora do ERP). Ele só precisa escrever o estado de
-- controle: baselines de schema drift e manifests de partição. Um volume externo dá
-- a esse prefixo um objeto próprio no Unity Catalog, com privilégio próprio
-- (READ/WRITE VOLUME). O acesso por URI de nuvem a um caminho de volume também passa
-- a obedecer aos privilégios do volume, não aos da external location.
--
-- Volumes não podem se sobrepor: prod fica com _control/ e dev sai para
-- _control_dev/ (o estado de dev é copiado pelo job de ops migrate_dev_control_root).
-- Resíduo aceito: _control/watermark_backup/ (backup da extração on-premises) fica
-- dentro do volume de prod até a extração v2 lhe dar um prefixo próprio.
-- Se algum objeto do Unity Catalog já ocupar o caminho, o CREATE falha: parar e investigar.

-- ── A) DEV — antes do deploy de dev que aponta o control_root para o volume ──
CREATE EXTERNAL VOLUME IF NOT EXISTS varejinho_dev.control.control_files
  LOCATION 's3://varejinho-lake/_control_dev/'
  COMMENT 'Estado de controle do pipeline em dev: baselines de schema drift, manifests de partição e sandboxes das fixtures';

-- ── B) PROD — no lote de deploy, depois do passo delete da migração de dev
--    (_control/dev/ já apagado) e antes do bundle deploy -t prod ──────────────
CREATE EXTERNAL VOLUME IF NOT EXISTS varejinho.control.control_files
  LOCATION 's3://varejinho-lake/_control/'
  COMMENT 'Estado de controle do pipeline em prod: baselines de schema drift e manifests de partição';

GRANT READ VOLUME, WRITE VOLUME ON VOLUME varejinho.control.control_files
  TO `bf71079b-64e1-4763-8b86-1e90718d8864`;   -- sp-varejinho-pipeline-prod
GRANT READ VOLUME ON VOLUME varejinho.control.control_files
  TO `varejinho-prod-readers`;

-- ── C) PROD — só depois do primeiro run verde lendo e escrevendo pelo volume ──
REVOKE READ FILES, WRITE FILES ON EXTERNAL LOCATION `varejinho_lake_new`
  FROM `bf71079b-64e1-4763-8b86-1e90718d8864`;

-- Conferência:
--   SHOW GRANTS ON EXTERNAL LOCATION `varejinho_lake_new`;     -- o SP não aparece
--   SHOW GRANTS ON VOLUME varejinho.control.control_files;      -- SP: READ VOLUME, WRITE VOLUME
-- Rollback de C (um comando, sem mexer em código):
--   GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `varejinho_lake_new`
--     TO `bf71079b-64e1-4763-8b86-1e90718d8864`;
