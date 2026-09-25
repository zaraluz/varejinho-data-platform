# Control storage migration to Unity Catalog volumes

Scopes the pipeline's file writes to its control state. Before: the production service principal held `READ FILES` and `WRITE FILES` on the external location `varejinho_lake_new` (`s3://varejinho-lake/`), the whole bucket, `bronze/` included. After: `READ VOLUME` and `WRITE VOLUME` on one external volume over `_control/`, and no file privilege on the bucket. SQL: [`ops/storage/control_volumes.sql`](../../ops/storage/control_volumes.sql). Decision: [decision log, 2026-09-25](../decision_log.md).

## Layout

| Environment | Volume (`control_root`) | Storage | Before |
|---|---|---|---|
| dev | `/Volumes/varejinho_dev/control/control_files` | `s3://varejinho-lake/_control_dev/` | `s3://varejinho-lake/_control/dev/`: copied, verified, then deleted |
| prod | `/Volumes/varejinho/control/control_files` | `s3://varejinho-lake/_control/` | Same prefix: production files do not move |

Volumes cannot overlap, so dev moves out of `_control/`. Production state stays where it is and only its access path changes. `_control/watermark_backup/`, written by the on-premises extraction, stays inside the production volume until extraction v2 gives it a prefix of its own.

## Interlocks of the migration job

- `dry_run=true` by default; `copy` and `delete` require `confirm_target=varejinho_dev`.
- The source is fixed to `_control/dev`; the target must be a volume of the dev catalog.
- `copy` refuses a non-empty target that differs from the source. If the target is already identical, it does nothing, so a failed run can be repeated.
- `copy` and `delete` check that no other run is active.
- `delete` checks again, at run time, that every source file exists in the target with the same size before removing anything.

## Dev

Run from `pipeline/`, on the branch with the change.

| # | Step | Command | Success criterion |
|---|---|---|---|
| 1 | Create the dev volume | Block A of `control_volumes.sql` in the SQL editor | `DESCRIBE VOLUME varejinho_dev.control.control_files` shows `s3://varejinho-lake/_control_dev/` |
| 2 | Deploy dev | `databricks bundle deploy -t dev` | Job "Ops Migrate Dev Control Root" exists. Do not run the dev pipeline before step 5 |
| 3 | Plan | `databricks bundle run -t dev migrate_dev_control_root --params step=plan` | Source lists `schema_registry`, `fact_partition_manifest` and the fixture sandboxes; target empty |
| 4 | Copy, dry run then real | `... --params step=copy`, then `... --params step=copy,dry_run=false,confirm_target=varejinho_dev` | Every source file present in the target with the same size |
| 5 | Verify | `... --params step=verify` | Files ✅; every manifest table has the same row count when read through the volume |
| 6 | Pipeline reads the volume | `databricks bundle run -t dev pipeline_diario` | Silver QG, Gold QG and dbt pass: drift baselines and manifests are read from the volume |
| 7 | Pipeline writes the volume | `databricks bundle run -t dev schema_drift_fixture`, then `databricks bundle run -t dev partition_manifest_fixture` | S2 and R2 pass: JSON baselines (`dbutils.fs.put`) and Delta manifests are written through the volume |
| 8 | Delete the old dev root, dry run then real | `... --params step=delete`, then `... --params step=delete,dry_run=false,confirm_target=varejinho_dev` | `_control/dev/` is gone; `step=plan` shows an empty source |

## Production

Part of the deploy batch after F9 closes, as its own deploy after the Gold release, so a failure points to one change.

| # | Step | Command | Success criterion |
|---|---|---|---|
| 9 | Create the volume and grant | Block B of `control_volumes.sql` (only after step 8) | `SHOW GRANTS ON VOLUME varejinho.control.control_files`: service principal `READ VOLUME`, `WRITE VOLUME`; readers group `READ VOLUME` |
| 10 | Deploy | `databricks bundle deploy -t prod` | `pipeline_diario` parameter `control_root` = `/Volumes/varejinho/control/control_files` |
| 11 | Run through the volume | `databricks bundle run -t prod pipeline_diario` | Silver QG, Gold QG and dbt pass |
| 12 | Revoke bucket access | Block C of `control_volumes.sql` | `SHOW GRANTS ON EXTERNAL LOCATION varejinho_lake_new` no longer lists the service principal |
| 13 | Prove least privilege | `databricks bundle run -t prod pipeline_diario` (or the next scheduled run, confirmed by `vigia_atualizacao` at 07:00) | Same gates pass with no file privilege on the bucket |

Between steps 9 and 10 a scheduled run still works: the deployed code uses the `s3://` path, and access to a volume's prefix through its cloud URI follows the volume's grants, which the service principal already has.

Least privilege cannot be proven in dev, where jobs run as the operator with full rights. Step 13 is the proof.

A volume holds files, never tables: Unity Catalog refuses a table defined on a volume path (`Missing cloud file system scheme`). The R2 fixture simulates Bronze with an external table, so its source files live under `s3://varejinho-lake/_fixtures/dev/`, outside any volume; only the manifests under test stay in the control volume.

## Rollback

- Dev, before step 8: the old root is intact. Point `control_root` back to it and redeploy.
- Production, before step 12: redeploy the previous tag. The `s3://` path keeps working through the volume grant.
- Production, after step 12: the rollback `GRANT` at the end of `control_volumes.sql`. One statement, no code change.
