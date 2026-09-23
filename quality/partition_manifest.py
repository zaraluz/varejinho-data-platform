# quality/partition_manifest.py
# Canonical post-commit mutation guard for mature daily fact partitions.
#
# Storage:
#   <control_root>/fact_partition_manifest/<entity>
# as one Delta table per entity, avoiding write contention between parallel facts.
#
# Fingerprint:
#   deterministic SHA-256 over the sorted physical file set for each ingestion_date,
#   using _metadata.file_path + _metadata.file_modification_time.
#
# Policy:
# - bootstrap is explicit; runtime never invents missing committed manifests
# - every APPLY can verify all already-committed partitions before mutating Silver
# - VALIDATE stages fingerprints for newly validated partitions
# - COMMIT re-reads source metadata and promotes only if staged fingerprints still match
# - any late mutation, missing partition, extra historical partition, or changed file set blocks

from __future__ import annotations

from datetime import date
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class PartitionManifestViolation(Exception):
    pass


MANIFEST_SCHEMA = StructType(
    [
        StructField("entity", StringType(), False),
        StructField("ingestion_date", DateType(), False),
        StructField("fingerprint", StringType(), False),
        StructField("file_count", LongType(), False),
        StructField("max_file_modified_at", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("captured_at", TimestampType(), False),
        StructField("committed_at", TimestampType(), True),
    ]
)


class FactPartitionManifestGuard:
    def __init__(self, spark, dbutils, control_root: str):
        self.spark = spark
        self.dbutils = dbutils
        self.control_root = control_root.rstrip("/")
        self.root = f"{self.control_root}/fact_partition_manifest"

    def entity_path(self, entity: str) -> str:
        return f"{self.root}/{entity}"

    def _is_delta(self, path: str) -> bool:
        try:
            return DeltaTable.isDeltaTable(self.spark, path)
        except Exception:
            return False

    def _manifest_df(self, entity: str) -> DataFrame:
        path = self.entity_path(entity)
        if not self._is_delta(path):
            return self.spark.createDataFrame([], MANIFEST_SCHEMA)
        return self.spark.read.format("delta").load(path)

    def _observe(
        self,
        source_table: str,
        *,
        lower_exclusive: Optional[date] = None,
        upper_inclusive: Optional[date] = None,
    ) -> DataFrame:
        predicates = ["ingestion_date IS NOT NULL"]
        if lower_exclusive is not None:
            predicates.append(f"ingestion_date > DATE '{lower_exclusive}'")
        if upper_inclusive is not None:
            predicates.append(f"ingestion_date <= DATE '{upper_inclusive}'")
        where = " AND ".join(predicates)

        # One physical file contributes one tuple per logical ingestion_date.
        # DISTINCT prevents row count inside the file from changing the file-set hash.
        return self.spark.sql(
            f"""
            WITH physical_files AS (
                SELECT DISTINCT
                    ingestion_date,
                    CAST(_metadata.file_path AS STRING) AS file_path,
                    CAST(_metadata.file_modification_time AS TIMESTAMP) AS file_modified_at
                FROM {source_table}
                WHERE {where}
            )
            SELECT
                ingestion_date,
                sha2(
                    concat_ws(
                        '||',
                        sort_array(
                            collect_list(
                                concat_ws(
                                    '::',
                                    file_path,
                                    date_format(
                                        file_modified_at,
                                        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
                                    )
                                )
                            )
                        )
                    ),
                    256
                ) AS fingerprint,
                CAST(count(*) AS BIGINT) AS file_count,
                max(file_modified_at) AS max_file_modified_at
            FROM physical_files
            GROUP BY ingestion_date
            """
        )

    def _compare_exact(
        self,
        expected: DataFrame,
        observed: DataFrame,
        *,
        context: str,
    ) -> dict:
        e = expected.select(
            "ingestion_date",
            F.col("fingerprint").alias("expected_fingerprint"),
            F.col("file_count").alias("expected_file_count"),
        ).alias("e")
        o = observed.select(
            "ingestion_date",
            F.col("fingerprint").alias("observed_fingerprint"),
            F.col("file_count").alias("observed_file_count"),
        ).alias("o")

        joined = e.join(o, on="ingestion_date", how="full_outer")

        missing = joined.filter(F.col("observed_fingerprint").isNull()).count()
        extra = joined.filter(F.col("expected_fingerprint").isNull()).count()
        changed = joined.filter(
            F.col("expected_fingerprint").isNotNull()
            & F.col("observed_fingerprint").isNotNull()
            & (
                (F.col("expected_fingerprint") != F.col("observed_fingerprint"))
                | (F.col("expected_file_count") != F.col("observed_file_count"))
            )
        ).count()

        report = {
            "context": context,
            "missing_partitions": missing,
            "extra_partitions": extra,
            "changed_partitions": changed,
            "ok": missing == 0 and extra == 0 and changed == 0,
        }

        if not report["ok"]:
            print(f"\n[MUTATION_GUARD] {context} FAIL: {report}")
            (
                joined.filter(
                    F.col("expected_fingerprint").isNull()
                    | F.col("observed_fingerprint").isNull()
                    | (F.col("expected_fingerprint") != F.col("observed_fingerprint"))
                    | (F.col("expected_file_count") != F.col("observed_file_count"))
                )
                .orderBy("ingestion_date")
                .show(50, truncate=False)
            )
            raise PartitionManifestViolation(
                f"{context}: committed/validated Bronze partition changed "
                f"(missing={missing}, extra={extra}, changed={changed})"
            )

        return report

    def bootstrap(self, entity: str, source_table: str, committed: date) -> dict:
        if committed is None:
            raise PartitionManifestViolation(
                f"{entity}: cannot bootstrap manifest without committed watermark"
            )

        path = self.entity_path(entity)
        existing = self._manifest_df(entity)
        existing_count = existing.count()

        if existing_count:
            # Explicit bootstrap is idempotent but never overwrites history.
            report = self.assert_committed_unchanged(entity, source_table, committed)
            return {
                "entity": entity,
                "created": False,
                "rows": existing_count,
                "verification": report,
            }

        observed = self._observe(source_table, upper_inclusive=committed)
        rows = observed.count()
        if rows == 0:
            raise PartitionManifestViolation(
                f"{entity}: no source partitions found through committed={committed}"
            )

        committed_present = (
            observed.filter(F.col("ingestion_date") == F.lit(committed)).count() == 1
        )
        if not committed_present:
            raise PartitionManifestViolation(
                f"{entity}: committed partition {committed} not found in source"
            )

        payload = (
            observed.withColumn("entity", F.lit(entity))
            .withColumn("status", F.lit("COMMITTED"))
            .withColumn("captured_at", F.current_timestamp())
            .withColumn("committed_at", F.current_timestamp())
            .select(*[field.name for field in MANIFEST_SCHEMA.fields])
        )
        payload.write.format("delta").mode("overwrite").save(path)

        verification = self.assert_committed_unchanged(
            entity, source_table, committed
        )
        return {
            "entity": entity,
            "created": True,
            "rows": rows,
            "verification": verification,
        }

    def assert_committed_unchanged(
        self,
        entity: str,
        source_table: str,
        committed: Optional[date],
    ) -> dict:
        if committed is None:
            return {
                "entity": entity,
                "committed": None,
                "manifest_rows": 0,
                "ok": True,
            }

        path = self.entity_path(entity)
        if not self._is_delta(path):
            raise PartitionManifestViolation(
                f"{entity}: committed={committed} but manifest is missing at {path}; "
                "run explicit manifest bootstrap before runtime"
            )

        expected = (
            self._manifest_df(entity)
            .filter(
                (F.col("status") == "COMMITTED")
                & (F.col("ingestion_date") <= F.lit(committed))
            )
        )
        manifest_rows = expected.count()
        if manifest_rows == 0:
            raise PartitionManifestViolation(
                f"{entity}: no COMMITTED manifest rows through {committed}"
            )

        unexpected_committed_ahead = (
            self._manifest_df(entity)
            .filter(
                (F.col("status") == "COMMITTED")
                & (F.col("ingestion_date") > F.lit(committed))
            )
            .count()
        )
        if unexpected_committed_ahead:
            raise PartitionManifestViolation(
                f"{entity}: manifest has {unexpected_committed_ahead} COMMITTED "
                f"partition(s) ahead of watermark {committed}"
            )

        observed = self._observe(source_table, upper_inclusive=committed)
        report = self._compare_exact(
            expected,
            observed,
            context=f"{entity}:committed<={committed}",
        )
        report.update(
            {
                "entity": entity,
                "committed": committed,
                "manifest_rows": manifest_rows,
            }
        )
        print(
            f"[MUTATION_GUARD] {entity}: committed history unchanged "
            f"through {committed} | partitions={manifest_rows}"
        )
        return report

    def stage_validated(
        self,
        entity: str,
        source_table: str,
        committed: Optional[date],
        candidate: date,
    ) -> dict:
        if candidate is None:
            raise PartitionManifestViolation(f"{entity}: candidate is required")
        if committed is not None and candidate <= committed:
            raise PartitionManifestViolation(
                f"{entity}: candidate={candidate} must be after committed={committed}"
            )

        self.assert_committed_unchanged(entity, source_table, committed)

        observed = self._observe(
            source_table,
            lower_exclusive=committed,
            upper_inclusive=candidate,
        )
        observed_count = observed.count()
        if observed_count == 0:
            raise PartitionManifestViolation(
                f"{entity}: no new source partitions found for candidate={candidate}"
            )
        if (
            observed.filter(F.col("ingestion_date") == F.lit(candidate)).count()
            != 1
        ):
            raise PartitionManifestViolation(
                f"{entity}: candidate partition {candidate} not found exactly once"
            )

        path = self.entity_path(entity)
        if not self._is_delta(path):
            # Only valid for an entity with no prior committed watermark.
            if committed is not None:
                raise PartitionManifestViolation(
                    f"{entity}: manifest missing before stage; bootstrap required"
                )
            empty = self.spark.createDataFrame([], MANIFEST_SCHEMA)
            empty.write.format("delta").mode("overwrite").save(path)

        manifest = self._manifest_df(entity)
        existing_pending = manifest.filter(F.col("status") == "PENDING_VALIDATION")
        pending_count = existing_pending.count()

        if pending_count:
            # Retry is allowed only when the already-staged set is exactly the same.
            self._compare_exact(
                existing_pending,
                observed,
                context=f"{entity}:pending-retry<={candidate}",
            )
            return {
                "entity": entity,
                "candidate": candidate,
                "staged_rows": pending_count,
                "reused": True,
            }

        payload = (
            observed.withColumn("entity", F.lit(entity))
            .withColumn("status", F.lit("PENDING_VALIDATION"))
            .withColumn("captured_at", F.current_timestamp())
            .withColumn("committed_at", F.lit(None).cast("timestamp"))
            .select(*[field.name for field in MANIFEST_SCHEMA.fields])
        )

        target = DeltaTable.forPath(self.spark, path)
        (
            target.alias("t")
            .merge(payload.alias("s"), "t.ingestion_date = s.ingestion_date")
            .whenNotMatchedInsertAll()
            .execute()
        )

        staged = self._manifest_df(entity).filter(
            F.col("status") == "PENDING_VALIDATION"
        )
        self._compare_exact(
            staged,
            observed,
            context=f"{entity}:stage-validated<={candidate}",
        )

        print(
            f"[MUTATION_GUARD] {entity}: staged {observed_count} validated "
            f"partition(s) through candidate={candidate}"
        )
        return {
            "entity": entity,
            "candidate": candidate,
            "staged_rows": observed_count,
            "reused": False,
        }

    def promote_validated(
        self,
        entity: str,
        source_table: str,
        committed: Optional[date],
        candidate: date,
    ) -> dict:
        if candidate is None:
            raise PartitionManifestViolation(f"{entity}: candidate is required")

        self.assert_committed_unchanged(entity, source_table, committed)

        path = self.entity_path(entity)
        if not self._is_delta(path):
            raise PartitionManifestViolation(
                f"{entity}: manifest missing at commit time"
            )

        manifest = self._manifest_df(entity)
        pending = manifest.filter(F.col("status") == "PENDING_VALIDATION")
        pending_count = pending.count()

        # Idempotent recovery: manifests may already be promoted if a previous
        # attempt failed after manifest promotion but before watermark UPDATE.
        if pending_count == 0:
            promoted = manifest.filter(
                (F.col("status") == "COMMITTED")
                & (F.col("ingestion_date") > F.lit(committed))
                & (F.col("ingestion_date") <= F.lit(candidate))
            ) if committed is not None else manifest.filter(
                (F.col("status") == "COMMITTED")
                & (F.col("ingestion_date") <= F.lit(candidate))
            )
            current = self._observe(
                source_table,
                lower_exclusive=committed,
                upper_inclusive=candidate,
            )
            self._compare_exact(
                promoted,
                current,
                context=f"{entity}:already-promoted<={candidate}",
            )
            return {
                "entity": entity,
                "candidate": candidate,
                "promoted_rows": promoted.count(),
                "reused": True,
            }

        current = self._observe(
            source_table,
            lower_exclusive=committed,
            upper_inclusive=candidate,
        )
        self._compare_exact(
            pending,
            current,
            context=f"{entity}:pre-commit<={candidate}",
        )

        target = DeltaTable.forPath(self.spark, path)
        target.update(
            condition="status = 'PENDING_VALIDATION'",
            set={
                "status": F.lit("COMMITTED"),
                "committed_at": F.current_timestamp(),
            },
        )

        print(
            f"[MUTATION_GUARD] {entity}: promoted {pending_count} manifest "
            f"partition(s) through candidate={candidate}"
        )
        return {
            "entity": entity,
            "candidate": candidate,
            "promoted_rows": pending_count,
            "reused": False,
        }
