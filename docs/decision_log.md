# Decision Log — Varejinho Data Platform

This file records architectural and operational decisions already made during the hardening of the platform. It is intentionally decision-oriented: what was decided, why, and what that implies for future work.

> Current release state: all decisions below apply to `feature/platform-hardening` / `--target dev` unless explicitly stated otherwise. `main` and production are not yet the source of truth for the hardened architecture.

## 2026-09-17 — Dev isolation uses a shared raw Bronze and isolated Silver/Gold

**Decision**
- Keep `varejinho` as the single owner of the physical Bronze external tables.
- Expose Bronze in `varejinho_dev` through read-only views.
- Keep `varejinho_dev.silver` and `varejinho_dev.gold` physically isolated.

**Why**
Unity Catalog does not allow registering the same physical external path twice (`LOCATION_OVERLAP`). Duplicating raw files only to simulate dev would add cost and create a second source of truth.

**Consequence**
Development can read the same raw source while all transformed state remains isolated. Environment separation is a catalog/runtime concern, not a duplicate-raw-data concern.

---

## 2026-09-17 — Product, supplier and merchandise hierarchy are owned by the SCD2 runtime

**Decision**
- `produto`, `fornecedor` and `mercadologico` are handled only by `incremental_scd2.py`.
- Reference/SCD1 dimensions stay in `transform_reference_dimensions.py`.
- The legacy `transform_dimensions.py` is not allowed to own those SCD2 entities in the official DAG.

**Why**
A DROP/rebuild of the latest snapshot is not SCD Type 2 even if columns such as `valid_from` and `is_current` exist. History must be persisted and replay-safe.

**Consequence**
The three historical dimensions have explicit version ownership, watermarks and validation before commit.

---

## 2026-09-17 — Type 1 / Type 2 semantics are business decisions, not “all changed columns become history”

**Decision**
- Product: structural/analytical identity attributes are Type 2; descriptive/current-state attributes such as `descricaoreduzida` are Type 1.
- Supplier: `cnpj` and `razaosocial` are Type 2; operational/descriptive attributes are Type 1.
- Merchandise hierarchy: hierarchy/path movement is Type 2; `descricao` is Type 1.

**Why**
A new historical version is justified only when an old fact must continue seeing the old attribute value.

**Consequence**
SCD2 is treated as a modeling contract about historical meaning, not a generic SQL pattern.

---

## 2026-09-17 — SCD2 reappearance is fail-fast until a business policy exists

**Decision**
If an ID disappears from a snapshot and later reappears, the runtime raises an error instead of inventing a reactivation rule.

**Why**
Reappearance could mean reactivation, extraction defect, source cleanup, or identity reuse. Those meanings are not interchangeable.

**Consequence**
The pipeline prefers an explicit failure over silently manufacturing historical semantics. Reappearance policy remains a Release Gate edge case.

> **Superseded on 2026-09-23** by *SCD2 reappearance follows the last observed state, not the absence*.

---

## 2026-09-18 — Fact watermark writes are serialized

**Decision**
Serialize fact/SCD2 chains that write to the same control table instead of parallelizing all commits.

**Why**
The small execution-time gain from parallel control-table writes is not worth increasing Delta write-conflict risk and operational blast radius.

**Consequence**
The official DAG favors deterministic state transitions over maximum task parallelism.

---

## 2026-09-18 — Absence from a later snapshot is not a business delete

**Decision**
Silver fact MERGEs perform update/insert only. Missing keys in a later source snapshot do not trigger deletes.

**Why**
The available source does not provide a trustworthy delete event. Treating absence as deletion would conflate extraction behavior with business state.

**Consequence**
Historical keys remain in Silver unless a future explicit delete semantic is introduced.

---

## 2026-09-21 — Daily partition existence does not mean daily partition maturity

**Decision**
A fact partition `D` is processable only when all currently visible files for `D` have a `file_modification_time` date later than `D`.

`mature_cutoff = max(ingestion_date where min(file_modification_date) > ingestion_date)`

Silver processes only:

`committed < ingestion_date <= mature_cutoff`

**Why**
Profiling proved that a folder for day `D` can exist while still being written and normally finalizes in `D+1`. Reading it merely because the folder exists moved open-partition rows into Silver.

**Consequence**
The platform has an explicit physical-readiness boundary rather than using visible Bronze max date as a transactional watermark.

> **Extended on 2026-09-23**: the rule is coupled to the extractor schedule and the UTC session timezone; see *D+1 maturity depends on the extractor schedule; timeliness is gated separately*.

---

## 2026-09-21 — Immutable Bronze / run identity is a fallback, not the active architecture

**Decision**
Keep the current Pentaho → S3 daily-partition landing architecture while the observed D+1 maturity invariant holds. Escalate to immutable object keys / batch identity only if evidence shows writes after the accepted maturity boundary or the current contract becomes unreliable.

**Why**
D5C showed all profiled fact tables compatible with the D+1 closure rule and no file modified after D+1 in the observed history.

**Consequence**
Do not redesign Bronze merely because an immutable-batch architecture is theoretically stronger. A post-commit mutation guard is still required before production release.

---

## 2026-09-21 — Fact state advances only through APPLY → VALIDATE → COMMIT

**Decision**
- APPLY may write Silver and sets `candidate_snapshot`.
- VALIDATE proves the new mature batch.
- COMMIT promotes the candidate to `last_processed_snapshot` only after validation succeeds.

**Why**
A successful write is not proof that the resulting state is correct. Watermark movement must represent a validated state transition.

**Consequence**
A failed validator does not falsely advance ingestion state, and retries can resume from an explicit pending state.

---

## 2026-09-21 — Repair contaminated fact baselines narrowly, not through blind full rebuilds

**Decision**
Repair only the affected maturity window / contaminated keys, preserve earlier valid history, validate, and only then realign the watermark.

**Why**
The defect was premature consumption of an open partition, not evidence that all prior Silver history was invalid.

**Consequence**
Repairs minimize blast radius and retain auditability through control/audit tables and Delta history.

---

## 2026-09-21 — `venda` follows the same maturity contract as the other 13 facts

**Decision**
Replace legacy full-scan `transform_sales.py` with `incremental_sales.py` using D+1 maturity, watermark and APPLY → VALIDATE → COMMIT.

**Why**
Real-data profiling proved `venda` had the same D+1 physical behavior and that its open partition had prematurely overwritten 6,802 existing IDs.

**Consequence**
All 14 Silver facts share the same incremental operating model. `transform_sales.py` was removed from the feature branch after real replay/equivalence tests passed.

---

## 2026-09-21 — Silver Facts are closed on incremental correctness, but Silver-wide hardening remains separate

**Decision**
Treat D7E as closure of the **Silver Facts incremental/maturity** workstream after the official dev pipeline passed Silver and Gold quality gates.

**Why**
Incrementality, maturity, replay and watermark semantics were proven independently from cross-cutting concerns such as contracts and schema drift.

**Consequence**
Do not reopen fact maturity gates unless a measurable regression appears. Contracts/drift are separate platform hardening concerns.

---

## 2026-09-21 — The first known SCD2 boundary is evidence-based, not synthetic history

**Decision**
For the initial known SCD2 version:
- `produto` uses `datacadastro` when reliable;
- `fornecedor` uses `datacadastro` when reliable;
- `mercadologico`, which has no trustworthy source timestamp, starts at the first observed ingestion date.

For later product changes, `dataalteracao` is used when reliable; supplier and merchandise hierarchy changes fall back to observed snapshot/ingestion time when the source does not expose a trustworthy change timestamp.

**Why**
The original initial load used `current_timestamp()`, which made historical facts precede every dimension version. Backdating the first known version to a trustworthy source timestamp repairs that load-boundary defect, but it does not magically reconstruct dimension history that the source never supplied.

**Consequence**
`valid_from` expresses the earliest defensible known boundary. It must not be moved further into the past merely to make every historical fact join successfully.

---

## 2026-09-21 — Gold historical facts must use the dimension version valid at the business event date

**Decision**
Use these temporal mappings:
- `fato_compras → dim_produto` by `datacompra`
- `fato_compras → dim_fornecedor` by `datacompra`
- `fato_promocoes → dim_produto` by `datainicio`
- `fato_contas_pagar → dim_fornecedor` by `dataemissao`
- `fato_outras_despesas → dim_fornecedor` by `dataemissao`

Existing facts already using justified temporal joins keep them.

**Why**
SCD2 only creates value if historical facts resolve to the dimension version that represented the event when it happened. The event date was chosen from business semantics, not from whichever date is convenient for partitioning.

**Consequence**
The previous `is_current=true` mappings for those relationships were replaced by interval joins (`event_ts >= valid_from` and `event_ts < valid_to`).

---

## 2026-09-21 — Temporal rows before the first known dimension version stay unresolved

**Decision**
If a fact occurs before the earliest historical version preserved for a dimension key, keep the `LEFT JOIN` result unresolved (`NULL`) instead of forcing a current-version fallback or rewriting `valid_from`.

**Why**
The missing historical coverage is a source-history limitation, not evidence that the current dimension value was true in the past.

**Consequence**
Gold quality checks distinguish explainable `before_first` gaps from unexpected temporal gaps. G3-style attempts to “fix” Silver history only to make every Gold join resolve are explicitly rejected.

---

## 2026-09-21 — Gold profiling may diagnose temporal coverage but must not become a second Silver audit

**Decision**
Use G1/G2 to prove the behavior of temporal joins and diagnose coverage boundaries, but do not reopen already-proven Silver SCD2 semantics without new contradictory evidence. The proposed G3 policy-impact profiler was removed once it became clear it would duplicate questions already settled in Silver.

**Why**
Gold owns **consumption** of SCD2 history; Silver owns **construction** of that history. Re-auditing `valid_from`, Type 1/2, watermark and replay inside Gold would blur ownership and create repeated work without a new failure mode.

**Consequence**
Gold temporal work is closed by proving correct fact-to-version mapping. Source-history limitations remain documented as limitations rather than being “fixed” downstream.

---

## 2026-09-22 — Data contracts use one canonical engine with explicit severity behavior

**Decision**
`quality/contract_engine.py` + `quality/contract_runtime.py` are the canonical contract implementation for active Silver runtimes.

Policy:
- structural/dataset-level contract failure → **FAIL CLOSED**
- row-level `error` violation → **QUARANTINE**
- `warning` → record/report without blocking
- mandatory contract missing → **FAIL CLOSED**

**Why**
A YAML is not a contract unless the declared interface is executable and interpreted identically by APPLY, VALIDATE and the Quality Gate.

**Consequence**
Inline/fail-open contract copies were removed from active incremental facts/sales paths. The same runtime semantics are reused across processing and validation.

---

## 2026-09-22 — Contract strength is proportional to data-product criticality

**Decision**
- 20 `critical/high` Silver entities have executable contracts.
- 17 `standard` entities intentionally use a simpler availability/structural gate rather than full YAML contracts.

**Why**
Forcing identical governance overhead onto every lookup table would add maintenance cost without equivalent risk reduction.

**Consequence**
The project does not claim “37/37 full contracts”; it documents differentiated controls by criticality.

---

## 2026-09-22 — Contracts describe the Silver interface that actually exists

**Decision**
Canonical YAML contracts must reflect the physical schema that the current Silver runtime actually produces. Contracts are not used to silently impose “better-looking” types, rename columns, or hide transformation changes.

Logical table/reference names are environment-independent (`table: venda`, `references: produto.id`); the runtime resolves `catalog` and `schema`.

**Why**
During C1, every existing contract had a type mismatch and several had stale columns or malformed decimal declarations. Enabling strict enforcement against aspirational schemas would have made the contract system itself the source of failures.

**Consequence**
A future change such as converting an ID from `string` to `int` must be implemented and tested as a Silver transformation/evolution change first, then reflected in the contract. It cannot be smuggled into production by editing YAML alone.

---

## 2026-09-22 — Snapshot uniqueness is scoped to `ingestion_date`

**Decision**
For current-state snapshot sources, the same business key on different ingestion dates is a legitimate update. A duplicate of the same key within the same `ingestion_date` is a true duplicate.

**Why**
Global uniqueness across snapshot history incorrectly quarantines valid state evolution.

**Consequence**
The contract runtime validates uniqueness with `uniqueness_scope=[ingestion_date]` before selecting the latest valid state per grain.

---

## 2026-09-22 — Contract row rules are enforced in APPLY; the Silver Quality Gate re-proves structure, not the entire dataset

**Decision**
- Row-level contract rules are enforced in the active APPLY runtime and violations are persisted to quarantine/reporting.
- The Silver Quality Gate revalidates the **structural contract** of the 20 `critical/high` entities and the availability of the 17 `standard` entities.
- The QG does not re-run every row-level rule over every full Silver table simply to duplicate APPLY behavior.

**Why**
A second full scan would create a parallel contract implementation in practice and add significant cost on large tables such as stock movement, without improving ownership clarity.

**Consequence**
The gate hierarchy is explicit: APPLY owns row-level enforcement, VALIDATE proves the candidate state, and Silver QG proves committed-state alignment plus structural contract integrity.

---

## 2026-09-22 — Schema drift detection and schema evolution are separate decisions

**Decision**
The next hardening block must not automatically overwrite the accepted schema baseline when drift is detected.

Target policy:
- detect and persist drift event
- classify (`additive`, `removed_column`, `type_change`, other breaking)
- apply policy by criticality
- promote a new baseline only after an explicit accepted evolution decision

**Why**
Observation that the schema changed is not permission for that new interface to become trusted Silver schema.

**Consequence**
The current `schema_drift.py` behavior that always overwrites the registry is considered technical debt and is the next canonical engineering block.

---

## 2026-09-22 — dbt documents/tests externally built Gold unless ownership moves to dbt

**Decision**
Current ownership remains: pipeline PySpark/SQL materializes Gold; dbt tests/documents it.

Therefore the dbt hardening must represent Gold as `sources` rather than pretending externally materialized relations are dbt models. Power BI becomes a dbt `exposure` when reconnected.

**Why**
Lineage and contracts must describe real ownership. Declaring a table as a dbt model does not make dbt its materialization owner.

**Consequence**
No migration of Gold ownership to dbt is implied by the dbt cleanup block. Such a migration would require a separate architectural decision.

---

## 2026-09-22 — `main` is a release boundary; production promotion waits for a Release Gate

**Decision**
Keep hardening in `feature/platform-hardening` / dev until the agreed core blocks are closed. Do not merge partial hardening into `main` simply because an individual gate passes.

Release sequence:
1. complete Schema Drift
2. complete dbt hardening
3. run Release Gate
4. review full feature vs `main` diff
5. remove/parameterize dev-only guards and legacy/experimental runtime references
6. validate bundles for dev and prod
7. ensure prod schedules remain `PAUSED`
8. PR + merge to `main`
9. manual prod deploy while paused
10. controlled production smoke test + quality gates
11. only then unpause schedules

**Why**
In the current repository, `main` is too close to the production release boundary to use it as an integration playground.

**Consequence**
Merge to `main` and activation of production schedules are separate approvals.

---

## 2026-09-22 — D+1 needs a post-commit mutation guard before production release

**Decision**
Before the Release Gate is passed, committed daily partitions must gain an auditable fingerprint/manifest (for example path, size, modification time and/or row-count/hash evidence) so later mutation of an already committed partition is detected.

**Why**
D+1 is strongly evidenced but still an operational assumption. A senior production design should detect when that assumption stops being true.

**Consequence**
A late mutation after commit becomes an explicit failure/alert instead of silently escaping the forward-only watermark.

---

## 2026-09-22 — Performance changes require measurement first

**Decision**
Do not migrate Gold to incremental processing, liquid clustering, or another storage/layout strategy merely because it is newer.

Measure first:
- full Gold rebuild duration/cost
- mature-cutoff discovery cost
- actual query/filter patterns
- current `PARTITION + ZORDER` behavior vs alternatives where relevant

**Why**
Correctness and simple reproducibility currently have higher value than speculative optimization.

**Consequence**
Performance work remains a benchmark-driven P2 task, not a prerequisite for the current correctness hardening.

---

## 2026-09-22 — Athena/Glue partition discovery is separate from Databricks maturity

**Decision**
Automate Athena partition discovery later through a scheduled Glue Crawler or evaluate Athena Partition Projection. Do not treat `MSCK REPAIR TABLE` as a permanent manual operating step.

**Why**
Athena/Glue catalog registration and Databricks physical-file maturity solve different problems. The current Databricks mature-cutoff logic reads physical files and `_metadata.file_modification_time` independently of Athena partition registration.

**Consequence**
AWS catalog automation cannot block current Databricks hardening unless the physical S3 file itself is missing.

---

## 2026-09-22 — Accuracy proof precedes BI reconnection

**Decision**
Before treating the analytical platform as finished for consumers, reconcile ERP/source metrics against Gold using independently calculated counts/financial metrics. Reconnect Power BI only after core quality/reconciliation work is closed.

**Why**
Schema checks, uniqueness and RI prove validity/consistency; they do not prove that analytical values match the business source of truth.

**Consequence**
ERP × Gold reconciliation is the main remaining accuracy gate. Genie and Power BI remain downstream validation/consumption steps, not substitutes for source reconciliation.

---

## 2026-09-22 — Data Vault is an optional learning slice, not a production requirement

**Decision**
Do not replace the Gold Star Schema with Data Vault just to demonstrate the pattern. If implemented, keep it as a small auditable learning case where lineage/history benefits justify it.

**Why**
Star Schema and Data Vault optimize different layers and goals. For this platform, forcing Data Vault into the serving layer would increase complexity without solving a current problem.

**Consequence**
Data Vault remains optional P2 learning work and must not distract from correctness/release gates.

---

## 2026-09-22 — The README is a public snapshot of validated state, not a marketing claim of future work

**Decision**
The repository README documents what has been proven on `feature/platform-hardening` and clearly labels Schema Drift, dbt hardening and the Release Gate as unfinished work.

**Why**
The repository is intended to function as technical portfolio evidence. Credibility is stronger when validated behavior, known limitations and open work are separated explicitly instead of presenting the target architecture as if it already existed.

**Consequence**
README updates should follow major validated gates. Pending architecture remains in the roadmap until an execution gate closes it.


---

## 2026-09-22 — Schema Drift is a control-plane decision over the accepted Silver interface

**Decision**
Use `quality/schema_drift_engine.py` as the canonical comparison/classification/promotion engine and `quality/schema_drift_runtime.py` as the active Silver adapter across all 37 entities.

Runtime policy:
- missing baseline → **FAIL CLOSED**
- no drift → **ALLOW**
- additive → persist event + **ALLOW WITH ACCEPTED-BASELINE PROJECTION**
- removed column / type change / mixed breaking → persist event + **BLOCK**
- baseline bootstrap and promotion are explicit operations only

For SCD2, evaluate drift against the **Silver-shaped output interface**, not raw Bronze. Bronze columns intentionally excluded from Silver are not schema evolution of the data product.

**Why**
The previous implementation coupled observation with acceptance: detecting drift could overwrite the baseline in the same run. SCD2 also proved that raw-source comparison can create false additive events when the Silver data product intentionally materializes only a subset of upstream columns.

**Consequence**
Detection and evolution are separate governance actions. Additive source change cannot silently enter Silver, breaking change cannot pass unnoticed, and upstream fields outside the published Silver interface do not create false drift.

Final evidence:
- central fixture: **6/6**
- D4 facts regression: **9/9**
- D7C sales regression: **11/11**
- reference/snapshot preflight: **20/20**
- final coverage: **37/37 entities**
- registry audit: **37/37 baselines exact vs Silver**, **37/37 column order aligned**, **0 invalid baselines**, **0 actionable findings**
- daily E2E: Silver QG **85/85**, Gold QG **51/51**

A synthetic `pedido` event created when the D4 fixture accidentally used the real dev control root was identified by its known event ID, removed as test pollution, and the registry was confirmed with **0 remaining events**.

---

## 2026-09-22 — Fixture control state must be isolated at the Job parameter boundary

**Decision**
When a Databricks Job defines a parameter also supplied by task `base_parameters`, isolated fixture roots must be set at the **Job-level parameter** that wins runtime precedence. A task-local sandbox path is not sufficient when the Job injects the production-like dev root under the same key.

**Why**
D4 initially compared a reduced synthetic `pedido` fixture schema against the real dev baseline, producing a false `mixed_breaking` event. The failure exposed parameter precedence rather than a data-product schema change.

**Consequence**
Schema Drift fixtures use their own control roots and may not write governance evidence into the real dev registry. Synthetic misrouted events are test pollution, not historical drift evidence.


---

## 2026-09-22 — dbt is a read-only validation/documentation layer over externally materialized Gold

**Decision**
Keep Gold materialization in the Databricks PySpark/SQL pipeline. Represent all 14 Gold relations in dbt as external `sources`; do not create phantom dbt models or set `materialized: table` for relations dbt does not build.

Environment resolution is delegated to the native Databricks `dbt_task` connection:
- dev → `varejinho_dev.gold`
- prod → `varejinho.gold`

No catalog is hardcoded in `sources.yml`.

**Why**
Ownership must match reality. The old project simultaneously declared Gold as sources and patched the same relations under `models:`, while `dbt_project.yml` hardcoded `varejinho.gold` and table materialization. That created misleading lineage and could cause a dev run to resolve prod objects.

**Consequence**
dbt now provides read-only tests, documentation metadata and lineage to externally built Gold. The legacy Python subprocess runner, tracked local log, phantom model YAML and duplicate singular test were removed. The canonical execution path is the native Databricks dbt task.

Final dev evidence:
- dbt Core **1.12.3**
- dbt-databricks **1.12.5**
- **14 Gold sources**
- **46 data tests**
- **44 PASS / 2 WARN / 0 ERROR / 0 SKIP**
- **0 deprecation warnings** after moving source metadata to `config.meta`

The two warnings are intentional business-anomaly monitors and do not represent pipeline failures.

---

## 2026-09-22 — dbt freshness and BI exposure require truthful runtime evidence

**Decision**
Do not configure dbt source freshness until Gold exposes a reliable technical load timestamp (or equivalent control signal) that represents the actual refresh time. Do not use a business date such as transaction date, event date or snapshot date as a fabricated `loaded_at_field`.

Do not declare Power BI as a dbt exposure until the consumer is actually reconnected to this Gold layer.

**Why**
Freshness and exposure metadata are useful only when they describe real runtime dependencies. Inventing a load timestamp from a business field would produce false freshness semantics, while declaring a disconnected BI consumer would create fictional lineage.

**Consequence**
The current dbt block is considered complete without source freshness or a Power BI exposure. Those features become follow-up work when the underlying runtime signals/dependencies exist.


---

## 2026-09-23 — Gold payable reconciliation is against the eligible fact grain

**Decision**
For `fato_contas_pagar`, reconcile Gold exactly against the Silver installment rows that have a matching `pagarfornecedor` header, using the actual fact grain `(id_parcela, id_loja)`. Do not compare Gold against all installments with an arbitrary percentage tolerance.

Track `pagarfornecedorparcela.id_pagarfornecedor -> pagarfornecedor.id` as a non-blocking referential-integrity warning in the Silver contract. In the Gold Quality Gate, fail if an orphan installment references a parent that exists in Bronze but is missing from Silver; if the parent is absent from Bronze as well, classify it as a source limitation.

**Why**
The Release Gate diagnostic found **840** Silver installments without a matching header, spanning **653** missing parent IDs. All **840** missing parents are absent from Bronze; none represent a parent present in Bronze but lost by Silver. The eligible Silver set has **75,913** rows and Gold has exactly **75,913** rows, with **0 eligible rows missing** and **0 extra Gold rows**.

The previous check (`Gold >= 99% of all Silver installments`) mixed pipeline correctness with source completeness and could pass or fail merely as the orphan ratio crossed an arbitrary threshold.

**Consequence**
Gold correctness is now fail-closed on exact eligible-grain reconciliation. Source-level orphan installments remain visible through contract warnings and the Gold source-provenance check, without being misclassified as a Gold transformation defect.


---

## 2026-09-23 — D+1 post-commit mutation guard is fail-closed and part of the release path

**Decision**
Treat a committed daily partition as immutable only while its accepted physical manifest still matches the source file set. The canonical guard stores one Delta manifest per fact entity under `<control_root>/fact_partition_manifest/<entity>`, fingerprinting each `ingestion_date` from the sorted combination of `_metadata.file_path` and `_metadata.file_modification_time`.

Runtime ordering is:
1. APPLY verifies all already-COMMITTED manifests before any Silver MERGE.
2. VALIDATE stages manifests for the newly validated candidate range.
3. COMMIT re-observes the source; any difference blocks the watermark.
4. Only an unchanged candidate manifest is promoted to COMMITTED.
5. A post-commit verification immediately rechecks the promoted history.
6. Silver Quality Gate independently re-proves committed-history immutability for all 14 incremental facts.

Manifest bootstrap is explicit and never silently overwrites an existing accepted baseline.

**Why**
The mature-partition rule prevents consuming an open daily partition, but a forward-only watermark alone cannot detect a source file that changes *after* that partition has already been committed. Without an accepted manifest, a late write could remain behind the watermark and silently escape future processing.

**Consequence**
Late mutation is now a blocking, auditable failure rather than an implicit D+1 assumption.

Closure evidence in dev:
- explicit real-history bootstrap: **14/14 manifests created and verified**
- isolated mutation fixture: **7/7**
- includes idempotent retry when manifest promotion succeeded but watermark update has not yet completed
- D4 integrated regression: **9/9**
- D7C integrated regression: **11/11**
- final daily E2E: Silver Quality Gate **99/99**
- Gold Quality Gate **52/52**
- all 14 fact watermarks committed through **2026-09-22** in the closing E2E

The accounts-payable Gold anomaly uncovered during this E2E was separately reconciled: **840** Silver installments reference **653** header IDs absent from Bronze; eligible Silver rows reconcile exactly to Gold with **0 missing** and **0 extra** rows.


---

## 2026-09-23 — SCD2 reappearance follows the last observed state, not the absence

**Decision**
When an ID disappears from one or more source snapshots and later reappears:
- absence is a non-event: no version is closed while the ID is missing;
- reappearing with the **same** Type 2 attributes creates no new version;
- reappearing with **changed** Type 2 attributes closes the previous version and opens a new one, exactly like any other Type 2 change;
- Type 1 changes on reappearance update current state without versioning;
- the watermark advances only after validation.

**Why**
The fail-fast placeholder (2026-09-17) was correct until the behavior was measured. The R3 profile found **0** real reappearances in the three dimensions, and the synthetic fixture proved the policy is consistent with an independent full backfill: incremental output equals full rebuild (`exceptAll` actual-only = 0, expected-only = 0), **11/11** checks. Treating absence as deletion would contradict the existing no-delete policy for snapshot sources.

**Consequence**
Reappearance no longer stops the pipeline. Closure evidence: R3 fixture 11/11, regressions on produto, fornecedor and mercadologico, and a daily E2E with Silver QG 99/99 and Gold QG 52/52.

---

## 2026-09-23 — D+1 maturity depends on the extractor schedule; timeliness is gated separately

**Decision**
1. Document that the maturity rule `to_date(file_modification_time) > ingestion_date` is evaluated in the Spark session timezone (**UTC**) and currently holds because the extractor's last daily fact load runs at **22:00 America/Fortaleza = 01:00 UTC of D+1**.
2. Add a timeliness check to the Silver Quality Gate: each incremental fact fails if its committed watermark is more than `max_fact_staleness_days` (default **2**) behind the current business date in America/Fortaleza. Normal lag is 1 day.
3. Do not change the maturity rule or the extractor schedule before the release. Changing them later is a single coordinated change (see Consequence).

**Why**
Verified on 2026-09-23 for `venda`, ingestion dates 2026-09-18 to 2026-09-22: the last write of every partition happened at 22:00 local / 01:00 UTC of the next day; the partition of the current day (last write 14:00 local) is correctly not mature.

The coupling was undocumented and creates a silent failure mode. If the last load of day D ever lands on D in UTC, for example a single morning load writing into the run-date folder, no partition ever matures. `committed` and `mature_cutoff` then stall together, the existing alignment check (`committed == mature_cutoff`) keeps passing, and the pipeline stays green while processing nothing. Only a comparison against the calendar detects this; it covers the *timeliness* dimension, which had no check.

**Alternatives considered**
- *Only reschedule the extractor*: this is exactly the change that triggers the failure mode if the maturity rule is not changed with it.
- *Change the maturity rule now*: it would reopen a closed gate (D4/D7C/R2 regressions) right before release for a rule that works today.

**Consequence**
A stalled maturity boundary becomes a blocking alert within two days instead of silent staleness. Post-release, when the extractor moves to one daily load (the ERP itself is D+1), change together: `ingestion_date` = business date, the extractor writes a `_SUCCESS` marker when a partition is complete, and maturity becomes "partition has `_SUCCESS`", removing the dependency on clock time and timezone.

---

## 2026-09-23 — Gold freshness is D-1 with one daily run at 03:00

**Decision**
Run `pipeline_diario` once a day at **03:00 America/Fortaleza** (previously 07:00, 13:00 and 21:00). Gold contains complete business days through **D-1**.

**Why**
- The ERP is itself D+1: a day is only complete in the source after it closes.
- Facts only advance on mature partitions. Extractor schedule: facts 06:00, 14:00 and 22:00; dimensions 02:00 (under 1 hour); domains 01:00. By 03:00, day D is mature (last write 22:00) and the day's dimension and domain snapshots are loaded.
- The 13:00 and 21:00 runs were no-ops for facts (`committed == mature_cutoff`), paying platform time for nothing.

**Consequence**
The legacy flow showed the current, incomplete day; the new Gold trades intraday freshness for complete days, a deliberate choice for a D+1 source. Sales recorded after the 22:00 load of day D are only captured by the next day's partition. If the extractor schedule changes, revisit this schedule together with the maturity rule above.

---

## 2026-09-23 — Runtime, one-off operations and validation evidence are separated

**Decision**
- `pipeline/`: only what the production jobs execute (16 notebooks + 14 Gold SQL files).
- `ops/`: one-off, destructive operations (bootstrap, SCD2 backfill, seeding, repairs), dev-guarded.
- `validation/`: fixtures, replays, profilers and diagnostics grouped by block (scd2, facts, sales, contracts, schema_drift, gold, release).
- The bundle is split: `databricks.yml` holds identity, variables, sync and targets; `resources/*.yml` holds the production jobs; `resources/dev/*.yml` declares ops and validation jobs **only under `targets.dev`**.
- Job resource keys are unchanged and every job carries `purpose`/`block` tags.

**Why**
72 files in one folder mixed 16 runtime notebooks with 56 proofs, and a production deploy would have created 50 jobs, including repairs and seeds. Renaming job keys would make the next deploy delete and recreate jobs, losing the run history that is the validation evidence.

**Consequence**
`bundle summary -t prod` resolves to the production jobs only. Fixtures still execute the runtime notebooks in `pipeline/`, so every gate tests the code that ships. Four notebooks whose results were cited without a job (C3, S1, S2, S3A) now have dev jobs.

---

## 2026-09-23 — Legacy code is deleted; rollback is a tag

**Decision**
Delete `transform_facts.py` and `transform_dimensions.py`. Create the annotated tag `legacy-v0` on the last `main` commit before the hardening release.

**Why**
No job referenced them. Keeping dead code "for rollback" confuses readers about what runs; Git already keeps it, and a tag gives the rollback point a stable name.

**Consequence**
Rollback or reference: `git show legacy-v0:pipeline/silver/transform_facts.py` or checkout the tag.

---

## 2026-09-23 — The deployment target is the only source of environment

**Decision**
- Runtime notebooks read `catalog`, `bundle_files_path`, `control_root` and `bronze_source_catalog` through `required_param()`: a missing value fails the task immediately.
- Bundle variables for these values have no default; each target must declare them.
- The `catalog must end with _dev` guard is removed from runtime notebooks and kept in `ops/` and `validation/`.
- `alert_email` is a bundle variable with no default, supplied outside Git (`.databricks/bundle/<target>/variable-overrides.json` or `BUNDLE_VAR_alert_email`). `warehouse_id` is resolved with `lookup` by warehouse name.

**Why**
Runtime notebooks silently fell back to different environments (Silver to `varejinho_dev`, Gold to `varejinho`) or to a personal workspace path. A configuration error would run against the wrong catalog instead of failing. The dev guard, useful during hardening, also prevented the runtime from ever running in prod.

**Consequence**
The same code runs in dev and prod. A static check over the bundle found **45** tasks that relied on the removed defaults before any job ran; they now receive the same values explicitly at job level (260/260 tasks covered, no task-level parameter overridden). Regressions D4 9/9, D7C 11/11, B7E 17/17. No personal or corporate email remains in the repository.

---

## 2026-09-23 — dbt runs as a dependency of Gold, from the deployed project

**Decision**
`dbt test` is the last task of `pipeline_diario`, after `gold_quality_gate`, with `source: WORKSPACE` (the dbt project deployed by the bundle). `dbt_tests` remains as an unscheduled manual job.

**Why**
The separate job ran one hour after each pipeline run, on a clock: a slow or failed run would be tested mid-rebuild or against stale Gold. `source: GIT` checked out whatever the branch held at run time, so an unreviewed push could change what ran without a deploy.

**Consequence**
Tests always target the Gold built by the same run, using the reviewed, deployed version. Evidence: E2E run `400101585433018` with dbt 44 PASS / 2 WARN / 0 ERROR (46 tests). The manual job keeps its run history (D2 evidence). `pipeline_diario` also gains `timeout_seconds: 7200` and a `RUN_DURATION_SECONDS > 3600` health rule with duration-warning notification; the measured run was about 40 minutes.

---

## 2026-09-23 — Production jobs run as a service principal from a restricted folder

**Decision**
- Target `prod` declares `run_as` with `sp-varejinho-pipeline-prod`, which has only Workspace and Databricks SQL entitlements (no admin, no consumer).
- Bundle files stay in the deployer's own folder; top-level permissions give the author `CAN_MANAGE` and the service principal `CAN_VIEW`.
- Unity Catalog grants are versioned in `ops/bootstrap/grant_prod_service_principal.sql`: `USE CATALOG`; read-only Bronze; `SELECT`, `MODIFY`, `CREATE TABLE` on Silver, Gold and control; `READ FILES`/`WRITE FILES` on the control-storage external location; warehouse `Can use`.

**Why**
Running production as the author couples it to a personal account, grants admin privileges to every job and blurs the audit trail. A first attempt placed files in `/Workspace/Shared`; `bundle validate` warned that it is writable by every workspace user. With `run_as` a service principal, anyone able to edit those notebooks would effectively run code with production privileges.

**Consequence**
Deploy identity and runtime identity are decoupled. Grants and ownership transfer of the cloned tables are applied during the cutover; the smoke test must confirm the service principal can read the bundle folder.

---

## 2026-09-23 — "Production" is the project release on Databricks Free Edition

**Decision**
The `prod` target is the release environment of this portfolio/academic project on Databricks Free Edition. The company's operational reporting does not depend on it; the legacy Pentaho/Athena/Power BI flow remains the operational system. Use of real company data requires the company's written approval, and the README states this scope explicitly.

**Why**
The Free Edition terms allow personal, academic and non-profit use, advise uploading only data one can afford to lose, and grant Databricks a broad license over uploaded content. The company does not fund a paid workspace. Keeping `prod` in the same bundle keeps a paid workspace a one-line change (`workspace.host` in the target).

**Consequence**
Governance is explicit rather than implied. Business-facing adoption is a separate decision that requires a paid workspace and a data-processing agreement.

---

## 2026-09-23 — Production cutover clones the validated state; rebuild is the DR path

**Decision**
Cut over by backing up the legacy prod Silver, then `DEEP CLONE`-ing the validated Silver, control and quarantine tables from `varejinho_dev` into `varejinho` with dev paused, copying the dev control storage (watermarks, schema baselines, partition manifests) to the prod control root, deploying prod paused, running once manually and gating on Silver QG, Gold QG and dbt. Rebuilding from scratch through `ops/` (bootstrap, backfill, seed) is documented as the disaster-recovery path.

**Why**
Production then starts exactly from the state that fixtures and E2E runs proved, instead of a new rebuild that would need to be proven again. The destructive `ops/` notebooks keep their dev-only guard.

**Consequence**
Rollback is renaming the legacy backup back. The detailed runbook is written at cutover time.

---

## 2026-09-23 — Repository conventions before release

**Decision**
- `.gitattributes` normalizes text files to LF on every platform.
- `pyproject.toml` pins Python (`>=3.11`) and ruff/pytest configuration; the baseline ruff run (318 findings, 196 line-length, 5 unused imports, 0 undefined names) is **not** applied before the release.
- No LICENSE file: code is published for portfolio review, all rights reserved; no business data is included (history scanned: no data files; fixtures use synthetic identifiers).

**Why**
Line endings produced whitespace-only diffs between Windows and Linux. Reformatting 80 files would bury the behavioral release diff. A permissive license would allow reuse of a real company's data model.

**Consequence**
Formatting, lint fixes and unit tests arrive with CI after the release.

---

## 2026-09-23 — Pipeline duration is dominated by platform wait, not by data volume

**Decision**
Do not optimize pipeline code before the release; revisit with a second measured run.

**Why**
Per-task timing of run `939249697177826` (39.5 min, 57 tasks): typical tasks take 5–30 s, but seven tasks took about 300 s each regardless of data volume (a 1,078-row commit took 307 s), in regular windows roughly 6.5 minutes apart, sometimes two unrelated branches stalling together. About 25 minutes of the critical path is waiting on the serverless platform. The earlier hypothesis (full `_metadata` scan of Bronze for maturity) does not explain the dominant cost.

**Consequence**
Timeout and duration alerts are sized for this behavior. A second measured run (`707938728538043`, 38.5 min, 58 tasks) confirmed the diagnosis: the stalled tasks were different from the first run (only `supplier_scd2_commit` repeated) and durations were quantized at about 300 s and 600 s (a 5-second commit took 606 s), which points to platform wait, not code. Post-release: consider fewer, coarser tasks per entity and measure again.

---

## 2026-09-23 — The release merges with a merge commit; versions stay release candidates until production proves them

**Decision**
- `feature/platform-hardening` merges into `main` with a merge commit. Squash and rebase merging are disabled in the repository settings.
- After the merge, `main` is tagged `v1.0.0-rc.1` (annotated tag, published as a GitHub pre-release). `v1.0.0` is tagged only after the production cutover passes the Silver quality gate, the Gold quality gate and dbt, on the commit that changes the README status to production. A defect found during cutover becomes `v1.0.0-rc.2`.
- `main` is protected by a ruleset: pull request required, no force push, no deletion. Linear history is not required, because it would forbid merge commits. Required approvals are 0 (single maintainer).

**Why**
Each commit on the feature branch is one step of a gate (for example, the timeliness check added to the Silver quality gate). Keeping them on `main` means `git log -- <file>` and `git blame` point to the exact step behind each guarantee; a squash would collapse 318 steps into a single commit. The project tracker also references individual commits, and squash or rebase merges would leave those references without a target once the branch is deleted. A release candidate is validated in the dev target but not yet in its destination; merge and production activation are separate approvals (see the 2026-09-22 entry on the release boundary).

**Consequence**
History is non-linear by design; `git log --first-parent main` shows one entry per merged release. The version string carries deployment status: a `-rc` suffix means validated in dev, a plain version means running in production.

---

## 2026-09-24 — The cutover runs as a versioned ops job, one step at a time

**Decision**
The production cutover is `ops/cutover/production_cutover.py`, exposed as the dev-only job `production_cutover` and run one step at a time (plan, backup, clone, verify, ownership, rollback). Every step defaults to a dry run, and writes require `confirm_target` to name the production catalog. The job runs as the operator, who owns both catalogs; the production service principal has no access to dev by design.

Findings from the read-only inventory shaped the steps:
- no production job existed and both scheduled dev jobs were paused;
- recent `last_altered` timestamps on production tables had no matching Delta commits (metadata maintenance, not writes);
- the production control root already held a legacy `schema_registry/`, the same path the new drift engine uses, so the legacy folders move to `_control_legacy/` before the validated state is copied;
- the dev control root also holds fixture sandboxes, so only `schema_registry/` and `fact_partition_manifest/` are copied;
- Gold is not cloned, because the first production run rebuilds it from the cloned Silver.

**Why**
A cutover typed into a console leaves no reviewable record and cannot be repeated. As code, it goes through a pull request, prints its full plan before writing, refuses to overwrite its own backup, and proves the result with checksums instead of spot checks.

**Consequence**
Rollback is a scripted step as long as the operator still owns the tables. The runbook lives in `docs/runbooks/production_cutover.md`.

## 2026-09-24 — `watermark_backup/` stays in the production control root

**Decision**
The cutover moves only the legacy `schema_registry/` to `_control_legacy/`. `watermark_backup/` stays where it is, and no cutover step touches it.

**Why**
The inventory classified the folder as legacy because no Databricks job and no table history wrote to it. The backup step showed otherwise. The move stopped at a file whose name contains `:`, which Hadoop paths cannot copy, and the listing taken to diagnose it showed one file per day up to the current date. The writer is the on-premises extraction, outside Databricks. Moving the folder would be undone by the extraction's next run, and the folder is not the platform's to move.

**Consequence**
Job lists and table history show who writes to tables; they do not show who writes to a storage prefix. Inventories of storage paths also check object modification times. Until extraction v2 gives the extraction its own prefix, `_control/` holds state from two owners: the platform (`schema_registry/`, `fact_partition_manifest/`) and the extraction (`watermark_backup/`).

## 2026-09-24 — Production is written only by the service principal and read through a group

**Decision**
After the cutover, every Silver, Gold and control table in production is owned by the service principal. People read production through the group `varejinho-prod-readers`, which holds `USE CATALOG` and `USE SCHEMA, SELECT` on the four schemas and nothing else, with the minimum workspace entitlement (Consumer access). The account that deploys the bundle holds the Service Principal: User role on the service principal.

**Why**
- The ownership step checks its own post-condition, and that check failed: owning the catalog and the schemas lets the operator grant privileges on the service principal's tables, not read them. Read access had to be granted explicitly.
- Granting to a group instead of a person keeps the versioned grants free of personal data, turns onboarding and offboarding into a membership change instead of a code change, and makes "who reads production" a single line in `SHOW GRANTS`.
- Read-only access for people separates duties: production changes only through the pipeline or a reviewed ops step. Writing as a person becomes an explicit act, such as transferring ownership back before a rollback.
- The first production deploy was rejected with 403, because binding a service principal to `run_as` requires an explicit role on it; otherwise anyone who can create a job could borrow its privileges. `bundle validate` does not check authorization.

**Consequence**
The runbook creates the group before the ownership step and names the deploy role before step 9. The first production run, executed as the service principal, passed the same gates as dev: Silver quality gate 113/113, Gold quality gate 52/52, dbt 44 pass / 2 warn / 0 error. The daily schedule stays paused until activation is decided. The service principal's `WRITE FILES` covers the whole lake bucket, not only control storage; narrowing it to an external volume is listed in the next steps.

## 2026-09-24 — Production runs on schedule

**Decision**
`pipeline_diario` (daily at 03:00, America/Fortaleza) and `manutencao_semanal` (Sundays at 02:00) are unpaused in the prod target of the bundle. Operations follow `docs/runbooks/daily_operations.md`.

**Why**
- The evidence for running unattended was in place: the first production run passed every gate as the service principal; failure emails had already reached the alert address from dev jobs, through the same mechanism; the run starts after the source closes the day, and D+1 maturity keeps the 02:00 dimension extraction out of the 03:00 run.
- The schedule changes through the bundle, not the job UI. The bundle is the source of truth: a UI toggle would be reverted silently by the next deploy, and it would leave no record of who changed it or why.
- Maintenance starts with the pipeline: each daily `CREATE OR REPLACE` of Gold leaves the previous files behind, and the weekly `VACUUM` bounds that growth.

**Consequence**
Activation counts as done after three consecutive green scheduled runs and the first weekly maintenance. Failure emails cover only runs that start; until a freshness alert exists, a morning glance at the run list covers a run that never started.
