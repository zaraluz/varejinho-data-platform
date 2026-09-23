# quality/partition_manifest_runtime.py
# Thin adapter around the canonical partition manifest engine.

from __future__ import annotations

import importlib.util


class FactPartitionManifestRuntime:
    def __init__(
        self,
        *,
        spark,
        dbutils,
        control_root: str,
        bundle_files_path: str,
        bronze_source_catalog: str,
    ):
        self.spark = spark
        self.dbutils = dbutils
        self.control_root = control_root.rstrip("/")
        self.bundle_files_path = bundle_files_path.rstrip("/")
        self.bronze_source_catalog = bronze_source_catalog

        engine_path = f"{self.bundle_files_path}/quality/partition_manifest.py"
        spec = importlib.util.spec_from_file_location(
            "varejinho_partition_manifest_engine_runtime",
            engine_path,
        )
        if spec is None or spec.loader is None:
            raise ImportError(
                f"Could not load partition manifest engine: {engine_path}"
            )

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        self.PartitionManifestViolation = module.PartitionManifestViolation
        self.guard = module.FactPartitionManifestGuard(
            spark=spark,
            dbutils=dbutils,
            control_root=self.control_root,
        )

    def source_table(self, entity: str, bronze_override: str = "") -> str:
        return (
            bronze_override
            if bronze_override
            else f"{self.bronze_source_catalog}.bronze.{entity}"
        )

    def assert_committed(
        self,
        entity: str,
        committed,
        *,
        bronze_override: str = "",
    ) -> dict:
        return self.guard.assert_committed_unchanged(
            entity,
            self.source_table(entity, bronze_override),
            committed,
        )

    def stage(
        self,
        entity: str,
        committed,
        candidate,
        *,
        bronze_override: str = "",
    ) -> dict:
        return self.guard.stage_validated(
            entity,
            self.source_table(entity, bronze_override),
            committed,
            candidate,
        )

    def promote(
        self,
        entity: str,
        committed,
        candidate,
        *,
        bronze_override: str = "",
    ) -> dict:
        return self.guard.promote_validated(
            entity,
            self.source_table(entity, bronze_override),
            committed,
            candidate,
        )
