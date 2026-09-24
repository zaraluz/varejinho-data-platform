# Daily operations runbook

How production runs unattended, how a failure reaches the operator, and what to do for each kind of failure. The one-time move to production is in the [cutover runbook](production_cutover.md).

## Schedule and alerts

| Job | Schedule (America/Fortaleza) | Alerts |
|---|---|---|
| `pipeline_diario` | Daily at 03:00 | Email on failure; email when a run passes 1 hour; hard timeout at 2 hours |
| `manutencao_semanal` | Sundays at 02:00 | Email on failure |
| `dbt_tests` | Manual only | None (the same tests run inside `pipeline_diario`) |

All production jobs run as the service principal, one run at a time. After a green daily run, Gold holds data up to the previous day (D-1). Dev jobs stay paused; running them never touches production.

**Silence is not success.** Failure emails only exist for runs that start. A run that never starts (for example, a deploy that paused the schedule) sends nothing, so glance at the job's run list in the morning: the latest run should be from today and green.

## Three rules

1. **Repair, do not roll back.** Every stage is safe to run again. APPLY skips the MERGE when a candidate is already pending validation, COMMIT does nothing when the watermark is already committed, and Gold is rebuilt with `CREATE OR REPLACE`. After fixing the cause, open the failed run and use **Repair run**: it re-runs the failed task and everything after it.
2. **Never edit control tables by hand.** Watermarks move only through APPLY → VALIDATE → COMMIT. A manual `UPDATE` breaks the guarantee every gate checks.
3. **Restoring data is a decision, not a routine.** It is for data that is wrong, not for a run that failed. Record it in the decision log before acting.

## Triage by failing task

| Failing task | What it means | What to do |
|---|---|---|
| `bronze_quality_gate` | The day's extract is incomplete, duplicated or missing | Check the extractor run and the day's folder on S3. Fix at the source, then repair. Nothing downstream ran. |
| `*_apply` (SCD2 or fact) | A `fail` contract, breaking schema drift, the mutation guard (a committed Bronze partition changed), or a runtime error | Read the message first. Breaking drift: promote a new baseline only after deciding the change is legitimate. Mutation guard: the source rewrote a closed day; investigate before anything else and do not commit over it. Transient error: repair. |
| `*_validate` | The applied candidate broke an invariant (grain, duplicates, SCD2 overlap) | Nothing was committed; the candidate stays pending. Find the cause and repair only once it is understood. Never force the commit. |
| `*_commit` | Promotion failed, usually a transient error | Repair. |
| `silver_quality_gate` | A Silver check failed, including timeliness (committed facts more than 2 days behind) | Timeliness failing while everything else is green means no partition matured: check the extractor schedule. |
| `gold_*`, `gold_quality_gate` | Gold build or its checks (unique keys, temporal foreign keys, reconciliation) | Silver is committed and correct. Fix and repair from the failed Gold task. The orphan-installment warning on `fato_contas_pagar` is an expected source limitation. |
| `dbt_test` | A test error (warnings do not fail the run) | Gold is already published. Fix and repair; warn consumers if the error affects them. |
| Duration warning | The run passed 1 hour | Usually serverless wait (see the README measurements). Act only if it repeats. |
| Timeout | The run was stopped at 2 hours | Repair. If it repeats, investigate before the next scheduled run. |

## Weekly maintenance

`manutencao_semanal` runs `OPTIMIZE`, `ZORDER` and `VACUUM` (30-day retention) on Gold. `VACUUM` is what matters: each daily `CREATE OR REPLACE` leaves the previous files behind, and without it storage only grows. Time travel on Gold reaches back 30 days.
