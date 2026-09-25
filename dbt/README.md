# dbt — Gold validation and documentation

This dbt project does **not** materialize the Varejinho Gold layer.

Gold tables and dimensions are built by the Databricks PySpark/SQL pipeline. dbt treats those relations as external `sources` and provides an additional read-only layer for:

- generic structural tests on stable Gold keys and dimensions;
- singular business/anomaly tests;
- documentation metadata;
- lineage from tests to the Gold relations they validate.

## Environment resolution

The Databricks Asset Bundle owns the environment:

- dev job target -> `varejinho_dev.gold`
- prod job target -> `varejinho.gold`

The source YAML intentionally omits a hardcoded catalog. The native Databricks `dbt_task` supplies the catalog and schema through its SQL warehouse connection.

No `profiles.yml` is committed. The Databricks dbt task uses `warehouse_id`, so the job provides the connection profile at runtime.

## Ownership boundary

```text
Databricks PySpark/SQL
    -> materializes Gold
    -> Gold Quality Gate

dbt
    -> reads Gold as sources
    -> tests
    -> documents
    -> does not CREATE/REPLACE Gold tables
```

This is deliberate: declaring an externally built Gold table as a dbt model would create false ownership and misleading lineage.

## Tests

The project contains:

- source-level generic tests for stable surrogate keys and safe non-null constraints;
- `relationships` tests from fact keys to the domain and promotion dimensions (loss reason, offer type, promotion, payment type on both financial facts, entry type);
- singular tests for calendar FK validity, SCD2 current-row consistency, zero-value sales with quantity, and pricing/margin anomaly monitoring.

Known temporal gaps that are valid by design are **not** converted into generic `not_null` failures. For example, some historical facts legitimately retain a null temporal surrogate key when the business event predates the first defensible dimension version.

## Validation evidence

Latest native Databricks dbt task in `dev` (star-schema conformance, 25/09/2026):

- Gold sources discovered: `19` (9 facts, 9 dimensions and the merchandise-tree reference)
- data tests discovered: `63` (57 generic, 6 singular)
- result: **61 PASS / 2 WARN / 0 ERROR / 0 SKIP**
- first validated with dbt Core `1.12.3` and dbt-databricks `1.12.5`; the job pins `dbt-databricks>=1.8.0,<2.0.0`
- no dbt deprecation warnings after moving source metadata to `config.meta`

The two warnings are intentional business-anomaly monitors:

- offer price above normal price;
- offer price below estimated average cost.

They are warnings rather than technical pipeline failures.

## Freshness

Gold currently does not persist a dedicated technical load timestamp on every relation. dbt source freshness is therefore not configured with a fabricated business-date field.

Freshness will be added only when the project has a reliable load-time signal that represents the actual Gold refresh.

## Exposure

Power BI is intentionally not declared as a dbt exposure yet because the consumer reconnection is still downstream of the current hardening/reconciliation gates. Add the exposure when that dependency is real again.

## Run in Databricks

In the daily flow, dbt runs as the last task of `pipeline_diario`, after `gold_quality_gate`: tests always run against the Gold that the same run just built, never on a clock that could fire before Gold finishes. The project is deployed with the bundle (`source: WORKSPACE`), so the tests that run are exactly the committed version that was deployed, not whatever the Git branch holds at run time.

To rerun only the tests against the current Gold, use the manual job:

```bash
cd pipeline

databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run dbt_tests --target dev
```

Both use a native Databricks `dbt_task` against a SQL warehouse resolved by name (`lookup`); the removed legacy Python subprocess runner is not part of the architecture.
