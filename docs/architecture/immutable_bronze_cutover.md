# Immutable Bronze Cutover Design

Status: **superseded by Gate D5C for the current pipeline. Kept as an escalation design if partitions are ever observed changing after D+1.**

## Why this design was considered

Gate D5B initially suggested that immutable batch identity might be required. Gate D5C then showed a consistent operational pattern across all 13 facts: current-day partitions are still open, while historical files are finalized no later than D+1. Because no file was observed changing after D+1, the current hardening path uses a closed-partition rule instead of changing Pentaho/S3 naming.

This document remains the fallback architecture if a future gate proves modifications later than D+1.

## Why this change exists

Gate D5 exposed a mismatch between the incremental Silver result and a full rebuild from the current external CSV Bronze.

Gate D5A/D5B proved that the root issue is upstream source mutability, not value transformation:

- shared keys matched exactly (`value_mismatch = 0`);
- 502 keys persisted in Silver but disappeared from the currently visible Bronze;
- one key (`3657528`) appeared later inside logical `ingestion_date = 2026-09-18`;
- the same logical partition/file path can therefore change after a watermark has already considered that date processed.

`ingestion_date` is a logical data attribute. It is **not** a safe immutable batch identity.

## Target architecture

PostgreSQL ERP -> Pentaho extract -> S3 immutable run files -> Delta Bronze append-only -> Silver incremental by source_batch_id -> Gold

### S3 object naming

Do not overwrite a fixed key such as:

`.../ingestion_date=2026-09-21/notaentradaitem.csv`

Write a unique object key for each extraction run instead:

`.../notaentradaitem/ingestion_date=2026-09-21/run_id=20260921T090220/notaentradaitem.csv`

`run_id` is operational identity. `ingestion_date` remains the logical source date.

### Defense in depth

Enable S3 Versioning on the raw bucket if available. Versioning is recovery/safety, not the primary batch identity.

The primary rule remains: **normal ingestion must never overwrite an existing raw object key.**

## Delta Bronze contract

Every captured row keeps the original payload plus:

- `_source_file_path`
- `_source_file_modification_time`
- `_source_file_size`
- `_source_batch_id`
- `_bronze_ingested_at`

Delta Bronze is append-only.

## Incremental boundary

Silver must stop using `last_processed_snapshot = ingestion_date` and move to an operational checkpoint equivalent to `last_processed_batch = source_batch_id`.

The Silver rule remains: existing business key -> update; new key -> insert; missing key in a later batch -> no automatic delete until deletion semantics are explicitly modeled.

## Why overwrite-aware ingestion is not enough

An ingestion tool can notice a changed object path, but intermediate overwritten versions can still be missed. Therefore unique producer-side object keys are the primary correctness fix.

## Cutover strategy

1. Preserve current Silver as pre-cutover observed history.
2. Capture the currently visible raw state into immutable Delta Bronze.
3. Run one-time late-arrival reconciliation: insert missing current raw keys, update changed keys, and do not delete legacy observed Silver keys merely because mutable raw lost them.
4. Start writing only immutable S3 run paths for new Pentaho exports.
5. Capture each new run into Delta Bronze.
6. Seed a batch-based Silver watermark at cutover.
7. Prove incremental == rebuild-from-immutable-Bronze.
8. Reconnect the official daily pipeline only after the new gates pass.

## Current failed state is intentional

`notaentradaitem`: committed=2026-09-18, candidate=2026-09-21, status=PENDING_VALIDATION.

Do not force-commit or reset it merely to make Gate D5 green.

## Acceptance gates

- D6A — immutable landing contract
- D6B — Delta Bronze capture
- D6C — cutover reconciliation
- D6D — Silver batch incremental
- D6E — replay/idempotency
- D6F — official pipeline integration