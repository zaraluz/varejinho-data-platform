# quality/contract_engine.py
# Engine canônico de Data Contracts para a camada Silver.
#
# Política:
# - erro estrutural / contrato inválido -> FAIL CLOSED
# - violação row-level com severity=error -> QUARANTINE
# - warning -> registra no relatório, não remove a linha
# - referências são lógicas (ex.: produto.id) e resolvidas pelo catalog/schema do job

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class ContractViolation(Exception):
    """Falha estrutural/dataset-level que deve bloquear o pipeline."""


class ContractValidator:
    SUPPORTED_RULES = {
        "not_null",
        "no_duplicates",
        "referential_integrity",
        "freshness",
    }
    SUPPORTED_SEVERITIES = {"error", "warning"}

    def __init__(
        self,
        contract_path: str,
        spark,
        catalog: str,
        schema: str = "silver",
    ):
        self.contract_path = contract_path
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

        try:
            with open(contract_path, "r", encoding="utf-8") as f:
                self.contract = yaml.safe_load(f) or {}
        except FileNotFoundError as exc:
            raise ContractViolation(
                f"Contrato obrigatório ausente: {contract_path}"
            ) from exc
        except Exception as exc:
            raise ContractViolation(
                f"Contrato YAML inválido: {contract_path}: {exc}"
            ) from exc

        self.table = str(self.contract.get("table", "") or "")
        self.columns = self.contract.get("columns", []) or []
        self.rules = self.contract.get("quality_rules", []) or []
        self.grain = self.contract.get("grain", []) or []

        self._validate_contract_definition()

    def _validate_contract_definition(self) -> None:
        if not self.table:
            raise ContractViolation("Contrato sem 'table'")
        if "." in self.table:
            raise ContractViolation(
                f"Contrato deve usar nome lógico de tabela, sem catálogo/schema: {self.table}"
            )
        if not isinstance(self.columns, list) or not self.columns:
            raise ContractViolation(f"{self.table}: contrato sem columns")
        if not isinstance(self.grain, list) or not self.grain:
            raise ContractViolation(f"{self.table}: contrato sem grain")

        names: List[str] = []
        for cfg in self.columns:
            if not isinstance(cfg, dict):
                raise ContractViolation(f"{self.table}: column config não é mapping")
            name = cfg.get("name")
            dtype = cfg.get("type")
            if not name or not dtype:
                raise ContractViolation(
                    f"{self.table}: toda coluna precisa de name + type"
                )
            names.append(str(name))

        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ContractViolation(
                f"{self.table}: colunas duplicadas no contrato: {duplicates}"
            )

        missing_grain = sorted(set(self.grain) - set(names))
        if missing_grain:
            raise ContractViolation(
                f"{self.table}: grain referencia colunas não declaradas: {missing_grain}"
            )

        for rule in self.rules:
            if not isinstance(rule, dict):
                raise ContractViolation(f"{self.table}: quality_rule não é mapping")
            rule_name = str(rule.get("rule", "") or "")
            severity = str(rule.get("severity", "") or "").lower()
            if rule_name not in self.SUPPORTED_RULES:
                raise ContractViolation(
                    f"{self.table}: rule não suportada: {rule_name}"
                )
            if severity not in self.SUPPORTED_SEVERITIES:
                raise ContractViolation(
                    f"{self.table}: severity inválida em {rule_name}: {severity}"
                )

    @staticmethod
    def _normalize_type(dtype: str) -> str:
        return str(dtype).lower().replace(" ", "")

    def _validate_dataframe_schema(self, df: DataFrame) -> None:
        actual = {
            field.name: self._normalize_type(field.dataType.simpleString())
            for field in df.schema.fields
        }
        declared = {
            str(cfg["name"]): self._normalize_type(cfg["type"])
            for cfg in self.columns
        }

        missing = sorted(set(declared) - set(actual))
        if missing:
            raise ContractViolation(
                f"{self.table}: coluna(s) obrigatória(s) ausente(s): {missing}"
            )

        mismatches = [
            f"{name}: contract={dtype}|actual={actual[name]}"
            for name, dtype in declared.items()
            if actual[name] != dtype
        ]
        if mismatches:
            raise ContractViolation(
                f"{self.table}: type mismatch: {mismatches}"
            )

    @staticmethod
    def _append_reason(df: DataFrame, condition, reason: str) -> DataFrame:
        return (
            df.withColumn(
                "_contract_invalid",
                F.when(condition, F.lit(True)).otherwise(F.col("_contract_invalid")),
            )
            .withColumn(
                "_contract_reason",
                F.when(
                    condition,
                    F.concat_ws(
                        "|",
                        F.col("_contract_reason"),
                        F.lit(reason),
                    ),
                ).otherwise(F.col("_contract_reason")),
            )
        )

    def _record_condition(
        self,
        df: DataFrame,
        condition,
        rule_name: str,
        severity: str,
        report_rows: List[Dict],
        detail: str,
    ) -> DataFrame:
        violations = df.filter(condition).count()
        report_rows.append(
            {
                "rule": rule_name,
                "severity": severity,
                "violations": violations,
                "detail": detail,
            }
        )
        if severity == "error" and violations:
            return self._append_reason(df, condition, f"{rule_name}:{detail}")
        return df

    def _resolve_reference(self, reference: str) -> Tuple[str, str]:
        parts = str(reference).split(".")
        if len(parts) != 2 or not all(parts):
            raise ContractViolation(
                f"{self.table}: referência deve ser lógica no formato tabela.coluna: {reference}"
            )
        ref_table, ref_column = parts
        physical = f"{self.catalog}.{self.schema}.{ref_table}"
        if not self.spark.catalog.tableExists(physical):
            raise ContractViolation(
                f"{self.table}: tabela referenciada ausente: {physical}"
            )
        if ref_column not in self.spark.table(physical).columns:
            raise ContractViolation(
                f"{self.table}: coluna referenciada ausente: {physical}.{ref_column}"
            )
        return physical, ref_column

    def validate(
        self,
        df: DataFrame,
        reference_time: Optional[datetime] = None,
    ) -> Tuple[DataFrame, DataFrame, Dict]:
        """
        Valida um lote já transformado/tipado.

        Retorna:
          (df_valid, df_quarantine, report)

        FAIL CLOSED ocorre via ContractViolation para erros estruturais e para
        regras dataset-level severity=error (ex.: freshness).
        """
        self._validate_dataframe_schema(df)

        work = (
            df.withColumn("_contract_invalid", F.lit(False))
            .withColumn("_contract_reason", F.lit(""))
        )
        report_rows: List[Dict] = []

        # Regras row-level declaradas diretamente em columns.
        explicit_not_null = {
            col
            for rule in self.rules
            if rule.get("rule") == "not_null"
            for col in (rule.get("columns") or [])
        }
        explicit_duplicate_keys = {
            tuple(rule.get("key") or [])
            for rule in self.rules
            if rule.get("rule") == "no_duplicates"
        }

        for cfg in self.columns:
            name = str(cfg["name"])

            if not bool(cfg.get("nullable", True)) and name not in explicit_not_null:
                work = self._record_condition(
                    work,
                    F.col(name).isNull(),
                    "not_null",
                    "error",
                    report_rows,
                    name,
                )

            if cfg.get("min") is not None:
                min_val = cfg["min"]
                work = self._record_condition(
                    work,
                    F.col(name).isNotNull() & (F.col(name) < F.lit(min_val)),
                    "min",
                    "error",
                    report_rows,
                    f"{name}>={min_val}",
                )

            if cfg.get("max") is not None:
                max_val = cfg["max"]
                work = self._record_condition(
                    work,
                    F.col(name).isNotNull() & (F.col(name) > F.lit(max_val)),
                    "max",
                    "error",
                    report_rows,
                    f"{name}<={max_val}",
                )

            accepted = cfg.get("accepted_values")
            if accepted is not None:
                work = self._record_condition(
                    work,
                    F.col(name).isNotNull() & (~F.col(name).isin(list(accepted))),
                    "accepted_values",
                    "error",
                    report_rows,
                    name,
                )

            if bool(cfg.get("unique", False)) and (name,) not in explicit_duplicate_keys:
                dup = F.count(F.lit(1)).over(Window.partitionBy(name)) > 1
                work = self._record_condition(
                    work,
                    dup,
                    "no_duplicates",
                    "error",
                    report_rows,
                    name,
                )

        # quality_rules com severity explícita.
        for idx, rule in enumerate(self.rules):
            rule_name = str(rule["rule"])
            severity = str(rule["severity"]).lower()

            if rule_name == "not_null":
                cols = list(rule.get("columns") or [])
                if not cols:
                    raise ContractViolation(f"{self.table}: not_null sem columns")
                missing = sorted(set(cols) - set(work.columns))
                if missing:
                    raise ContractViolation(
                        f"{self.table}: not_null referencia colunas ausentes: {missing}"
                    )
                condition = None
                for col_name in cols:
                    current = F.col(col_name).isNull()
                    condition = current if condition is None else (condition | current)
                work = self._record_condition(
                    work,
                    condition,
                    rule_name,
                    severity,
                    report_rows,
                    ",".join(cols),
                )

            elif rule_name == "no_duplicates":
                key = list(rule.get("key") or [])
                if not key:
                    raise ContractViolation(f"{self.table}: no_duplicates sem key")
                missing = sorted(set(key) - set(work.columns))
                if missing:
                    raise ContractViolation(
                        f"{self.table}: no_duplicates referencia colunas ausentes: {missing}"
                    )
                condition = F.count(F.lit(1)).over(Window.partitionBy(*key)) > 1
                work = self._record_condition(
                    work,
                    condition,
                    rule_name,
                    severity,
                    report_rows,
                    ",".join(key),
                )

            elif rule_name == "referential_integrity":
                column = str(rule.get("column", "") or "")
                reference = str(rule.get("references", "") or "")
                if not column or column not in work.columns:
                    raise ContractViolation(
                        f"{self.table}: RI com coluna local inválida: {column}"
                    )
                physical, ref_column = self._resolve_reference(reference)
                token = f"_contract_ref_{idx}"
                ref = (
                    self.spark.table(physical)
                    .select(F.col(ref_column).alias(token))
                    .distinct()
                    .withColumn(f"{token}_exists", F.lit(1))
                )
                work = work.join(
                    ref,
                    F.col(column) == F.col(token),
                    "left",
                )
                condition = F.col(column).isNotNull() & F.col(f"{token}_exists").isNull()
                work = self._record_condition(
                    work,
                    condition,
                    rule_name,
                    severity,
                    report_rows,
                    f"{column}->{reference}",
                ).drop(token, f"{token}_exists")

            elif rule_name == "freshness":
                column = str(rule.get("column", "") or "")
                max_age_hours = rule.get("max_age_hours")
                if not column or column not in work.columns:
                    raise ContractViolation(
                        f"{self.table}: freshness com coluna inválida: {column}"
                    )
                if max_age_hours is None:
                    raise ContractViolation(
                        f"{self.table}: freshness sem max_age_hours"
                    )

                max_ts = work.agg(F.max(F.col(column)).alias("max_ts")).collect()[0]["max_ts"]
                now = reference_time or datetime.now(timezone.utc).replace(tzinfo=None)
                stale = max_ts is None or ((now - max_ts).total_seconds() / 3600.0 > float(max_age_hours))
                report_rows.append(
                    {
                        "rule": rule_name,
                        "severity": severity,
                        "violations": 1 if stale else 0,
                        "detail": f"{column}:max_age_hours={max_age_hours}:max={max_ts}",
                    }
                )
                if stale and severity == "error":
                    raise ContractViolation(
                        f"{self.table}: freshness ERROR: {column} excedeu {max_age_hours}h"
                    )

        total = work.count()
        quarantine = (
            work.where(F.col("_contract_invalid"))
            .drop("_contract_invalid")
        )
        valid = (
            work.where(~F.col("_contract_invalid"))
            .drop("_contract_invalid", "_contract_reason")
        )
        quarantined = quarantine.count()

        warnings = [
            row for row in report_rows
            if row["severity"] == "warning" and row["violations"] > 0
        ]
        errors = [
            row for row in report_rows
            if row["severity"] == "error" and row["violations"] > 0
        ]

        report = {
            "table": self.table,
            "contract_path": self.contract_path,
            "total": total,
            "valid": total - quarantined,
            "quarantine": quarantined,
            "quarantine_percent": round(quarantined / total * 100, 4) if total else 0.0,
            "warning_count": len(warnings),
            "error_rule_count": len(errors),
            "warnings": warnings,
            "errors": errors,
            "rules": report_rows,
        }

        return valid, quarantine, report
