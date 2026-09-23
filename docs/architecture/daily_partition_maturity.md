# Daily Partition Maturity Policy

Status: **active hardening direction after Gate D5C**

## Evidence

Gate D5C profiled all 13 fact tables using `_metadata.file_modification_time` from the source external tables.

Observed pattern:

- minimum lag: 0 days;
- median lag: 1 day;
- maximum lag: 1 day;
- zero files with modification date later than D+1 in every fact;
- the current-day partition is visible before it is final;
- the previous-day partition is finalized around 01:00 on D+1.

Therefore `ingestion_date = D` is not considered closed merely because the folder exists.

## Closed-partition rule

A partition D is mature only when all currently visible source files for D have a `file_modification_time` whose calendar date is later than D.

In the current one-file-per-partition layout this is equivalent to:

`to_date(_metadata.file_modification_time) > ingestion_date`

The runtime uses the greatest mature `ingestion_date` as `mature_cutoff`.

## Silver processing

Silver processes only:

`last_processed_snapshot < ingestion_date <= mature_cutoff`

The current-day open partition is ignored even if it is already visible in S3.

After APPLY:

- `candidate_snapshot = mature_cutoff`;
- status becomes `PENDING_VALIDATION`;
- the committed watermark does not move until validation succeeds.

## Validation semantics

The validator checks the newly mature batch, not a full rebuild from mutable historical CSV.

It requires:

- every valid key in `(committed, candidate]` exists in Silver;
- payload for those keys matches exactly;
- no duplicate business keys in Silver;
- no Silver row has `ingestion_date > candidate`;
- historical Silver keys may remain even if they disappeared from mutable raw, consistent with the approved no-delete policy.

## Escalation condition

If any future observation shows a source file modified later than D+1, this policy is invalid for that table. Escalate to explicit immutable batch identity / versioned Bronze as documented in `immutable_bronze_cutover.md`.