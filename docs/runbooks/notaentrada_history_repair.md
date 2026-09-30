# `notaentrada` history repair runbook

One-time repair that brings back the `notaentrada` history Silver never received. Run it once per environment, dev first. The decision is in the [decision log](../decision_log.md) (2026-09-30).

## What went wrong

The full historical extract of `notaentrada` was written into the current day's Bronze folder (`bronze/notaentrada/ingestion_date=<today>/notaentrada.csv`). The daily extraction writes the same key, and S3 has no append: the last write wins. The daily file replaced the historical one the same day, before Silver read it. The D+1 maturity rule protects a partition from changes after it matures, not from being overwritten while it is still open. Silver kept only the entries the daily extracts carried.

Separately, before the grain became `id`, the merge on `numeronota + id_loja + id_fornecedor` fused different entries that share a note number. Those entries are still in Bronze, so the repair brings them back too.

## Operational rule

**Never write a backfill into a daily Bronze folder.** A backfill goes to its own prefix with a unique run id (`bronze_backfill/<table>/run_id=<UTC timestamp>/`), is registered as its own external table, and reaches Silver through a repair job like this one. The structural fix (unique file names per extraction run) is on the backlog.

## The rule the repair applies

The same rule as the daily pipeline: the most recent extraction wins.

| Entry in the source | Entry in Silver | Action |
|---|---|---|
| Present | Missing | Insert |
| Present, newer extraction | Present, older extraction | Update |
| Present, older or same extraction | Present | Leave as is |

Source = the recovered file (every row gets `ingestion_date = 2026-09-16`, the date it was extracted) plus the committed Bronze (`ingestion_date` up to the committed watermark). The recovered file and the daily folder `ingestion_date=2026-09-16` share the same extraction date; for an entry in both, the daily row wins, because the daily file is the one that overwrote the backfill (it is the later extraction). Every row passes the Silver contract first; rows that fail go to `_quarantine_history_notaentrada`, never to `_quarantine_notaentrada`, so the daily quality gate is not affected. The repair never deletes a row.

## Before you start

- `pipeline_diario` is not running and the `notaentrada` watermark is `COMMITTED` with no candidate. The job checks both and refuses otherwise. Run outside the 03:00–05:00 window.
- The grain change (`notaentrada` keyed by `id`) is in production. Running this before it would let the old composite-key merge fold restored entries again.

## Steps

### 1. Land the recovered file (once, shared by dev and prod)

In AWS CloudShell, copy the recovered version to its own prefix:

```bash
aws s3 cp \
  s3://varejinho-lake/_recovery/notaentrada/2026-09-16_backfill_39MB.csv \
  s3://varejinho-lake/bronze_backfill/notaentrada/run_id=20260916T172331Z/notaentrada.csv
```

Then run `ops/storage/notaentrada_backfill_landing.sql` in the SQL editor as the owner of `varejinho.bronze`.

**Check:** the query at the end of the SQL file returns as many rows as distinct ids, and `dataentrada` starts in October 2023.

### 2. Deploy and run in dev

Deploy the bundle to dev and run the job `[dev] Varejinho — Reparo do Histórico de Notas de Entrada` once per step, changing only the parameters listed:

| Run | Parameters | Expected |
|---|---|---|
| plan | defaults (`step=plan`) | Counts of inserts, updates and unchanged rows; invalid rows; orphan items today and after the apply. Nothing is written. |
| apply, dry run | `step=apply` | The same counts, marked `[dry-run]`. Nothing is written. |
| apply | `step=apply`, `dry_run=false`, `confirm_target=varejinho_dev`, `approved_by=<name>` | Prints the Silver version **before** the apply (keep it: rollback returns there), then the new version, and appends one row to `control.ops_repair_log`. |
| verify | `step=verify` | Every check green. |

What the checks mean:

- **ids unique**: the grain holds after the merge.
- **touched ids = planned inserts + updates**: the merge did exactly what the plan said, compared against the version it started from.
- **rows outside the repair unchanged**: nothing else moved.
- **touched rows = winning source state**: every inserted or updated row is the source row, value by value.
- **every valid source id is in Silver** and **no row went back to an older extraction**: the rule held both ways.
- **items without header = 0**: the symptom that started the investigation is gone.

Running `apply` a second time finds nothing to insert or update and writes nothing.

### 3. Run in prod

Deploy to prod (the job runs as the service principal) and repeat the four runs with `confirm_target=varejinho`.

### 4. After the repair

- The next `pipeline_diario` run must be green. The informational line "número de nota reutilizado" in the Silver quality gate goes up: restored entries include producer notes that reuse numbers. That is expected.
- Record the numbers from the plan and verify runs in the Notion decision log.
- Tag the release.
- Delete the recovered copies under `s3://varejinho-lake/_recovery/notaentrada/` only after the prod verify is green. The landing under `bronze_backfill/` stays: it is the repair's source.

## If verify fails

Do not run the pipeline. Read which check failed. To undo, run `step=rollback`, `rollback_version=<version printed before the apply>`, first as a dry run, then with `dry_run=false`, `confirm_target` and `approved_by`. Rollback restores Silver with `RESTORE TABLE`; rows written to the quarantine history stay there as a record.
