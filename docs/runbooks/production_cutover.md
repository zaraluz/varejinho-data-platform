# Production cutover runbook

Moves production (`varejinho`) from the legacy state to the state validated in dev (`varejinho_dev`). Code is deployed by the bundle; **state** is copied by the `production_cutover` ops job, one step per run. Decision: [decision log, 2026-09-23 and 2026-09-24](../decision_log.md).

## What moves and what does not

| Object | Action |
|---|---|
| `varejinho.bronze` (external tables on S3) | Untouched: dev already reads the same raw files |
| `varejinho.silver` (legacy) | Deep-cloned to `silver_legacy`, then replaced by `varejinho_dev.silver` |
| `varejinho.gold` (legacy) | Deep-cloned to `gold_legacy`; rebuilt by the first production run (not cloned) |
| `varejinho.control` | Created from `varejinho_dev.control` (fact and SCD2 watermarks, repair audits) |
| `s3://varejinho-lake/_control/schema_registry/` (legacy) | Moved to `s3://varejinho-lake/_control_legacy/` |
| `s3://varejinho-lake/_control/watermark_backup/` | Untouched: written daily by the on-premises extraction, not by Databricks |
| `s3://varejinho-lake/_control/dev/{schema_registry,fact_partition_manifest}` | Copied to `s3://varejinho-lake/_control/` |
| `_control/dev/` fixture sandboxes (`d4/`, `d7c/`, `_schema_drift_fixture/`) | Not copied |

## Interlocks

- `dry_run=true` by default: every step prints the exact statements and copies it would run.
- Any write requires `confirm_target=varejinho`.
- `backup` refuses to run if a backup already exists or the clone already happened, so the legacy copy can never be overwritten by the new state.
- `backup`, `clone` and `verify` check that no other job run is active and that every dev watermark is `COMMITTED` with no pending candidate: the clone is a consistent snapshot.
- Every copy is verified: tables by schema, row count and an `xxhash64` checksum over all columns; files by relative path and size.

## Steps

Run from `pipeline/`. `bundle run` passes job parameters with `--params`.

| # | Step | Command | Success criterion |
|---|---|---|---|
| 0 | Publish the job | `databricks bundle deploy -t dev` | Job "Ops Production Cutover" exists in dev |
| 1 | Plan (read-only) | `databricks bundle run -t dev production_cutover --params step=plan` | No active runs, all watermarks `COMMITTED`, no existing backup |
| 2 | Backup, dry run | `... --params step=backup` | Statement list matches the plan |
| 3 | Backup | `... --params step=backup,dry_run=false,confirm_target=varejinho` | Every legacy table cloned with identical checksum; legacy control moved |
| 4 | Clone, dry run | `... --params step=clone` | 37 Silver and 4 control tables; 2 control folders |
| 5 | Clone | `... --params step=clone,dry_run=false,confirm_target=varejinho` | Completes without error |
| 6 | Verify | `... --params step=verify` | Every table and folder ✅ |
| 7 | Grants | Run `ops/bootstrap/grant_prod_service_principal.sql` in the SQL editor | All statements succeed |
| 8 | Ownership | `... --params step=ownership,dry_run=false,confirm_target=varejinho` | Tables owned by the service principal; operator still reads |
| 9 | Deploy production | `databricks bundle validate -t prod` then `databricks bundle deploy -t prod` | 3 jobs, schedules `PAUSED` |
| 10 | First production run | `databricks bundle run -t prod pipeline_diario` | Silver QG, Gold QG and dbt pass, running as the service principal |

After step 10: README status badge to production, tag `v1.0.0` on that commit, GitHub Release as latest. Schedules stay paused until an explicit activation decision.

## Rollback

Before the first production run and before step 8 (the operator must still own the tables):

```
databricks bundle run -t dev production_cutover --params step=rollback,dry_run=false,confirm_target=varejinho
```

It re-clones Silver and Gold from the `_legacy` schemas, drops `varejinho.control`, removes the copied control folders and restores the legacy ones. After step 8, transfer ownership back first. After a production run, pause the production jobs before rolling back.

Delta time travel is a second line of defense: `CREATE OR REPLACE` keeps table history, so `RESTORE TABLE ... TO VERSION AS OF` works while the retention period lasts.
