# quality/contract_runtime.py
# Adaptador único entre os runtimes Silver incrementais e o contract_engine.
# Não contém regras de qualidade próprias: resolve contrato/engine, confere grain
# e prepara o latest candidate de uma tabela current-state antes da validação.

from __future__ import annotations

import importlib.util
import json
import os
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


_ENGINE_PATH = os.path.join(os.path.dirname(__file__), "contract_engine.py")
_spec = importlib.util.spec_from_file_location("varejinho_contract_engine", _ENGINE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Não foi possível carregar contract_engine: {_ENGINE_PATH}")
_engine = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_engine)

ContractValidator = _engine.ContractValidator
ContractViolation = _engine.ContractViolation


class SilverContractRuntime:
    def __init__(self, spark, catalog: str, bundle_files_path: str):
        self.spark = spark
        self.catalog = catalog
        self.bundle_files_path = bundle_files_path
        self.contract_dir = f"{bundle_files_path}/contracts/silver"

    def validator(self, entity: str, expected_grain: List[str]):
        contract_path = f"{self.contract_dir}/{entity}.yaml"
        validator = ContractValidator(
            contract_path,
            spark=self.spark,
            catalog=self.catalog,
            schema="silver",
        )

        if validator.table != entity:
            raise ContractViolation(
                f"Contrato incorreto para runtime {entity}: table={validator.table}"
            )

        actual_grain = list(validator.grain)
        if actual_grain != list(expected_grain):
            raise ContractViolation(
                f"{entity}: grain runtime={list(expected_grain)} diverge do contrato={actual_grain}"
            )
        return validator

    @staticmethod
    def latest_candidate(
        df: DataFrame,
        grain: List[str],
        snapshot_col: str = "ingestion_date",
    ) -> DataFrame:
        """
        Retém o snapshot mais recente por grain, mas preserva empates.

        Preservar empates é intencional: se duas linhas com o mesmo grain
        coexistirem no mesmo snapshot mais recente, o contract_engine deve
        enxergar ambas e a regra no_duplicates deve quarantinar a ambiguidade.
        """
        required = set(grain) | {snapshot_col}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ContractViolation(
                f"latest_candidate: colunas obrigatórias ausentes: {missing}"
            )

        token = "_contract_latest_snapshot"
        w = Window.partitionBy(*grain)
        return (
            df.withColumn(token, F.max(F.col(snapshot_col)).over(w))
            .where(F.col(snapshot_col) == F.col(token))
            .drop(token)
        )

    @staticmethod
    def normalize_quarantine(df: DataFrame) -> DataFrame:
        # Compatibilidade com as tabelas _quarantine_* já existentes.
        if "_contract_reason" in df.columns and "_motivo" not in df.columns:
            return df.withColumnRenamed("_contract_reason", "_motivo")
        return df

    @staticmethod
    def log_report(entity: str, report: dict) -> None:
        summary = {
            "table": report.get("table"),
            "total": report.get("total"),
            "valid": report.get("valid"),
            "quarantine": report.get("quarantine"),
            "warning_count": report.get("warning_count"),
            "error_rule_count": report.get("error_rule_count"),
            "warnings": report.get("warnings", []),
            "errors": report.get("errors", []),
        }
        print(
            f"[CONTRACT] {entity}: "
            + json.dumps(summary, ensure_ascii=False, default=str)
        )
