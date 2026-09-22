# quality/schema_drift_runtime.py
# Adaptador único entre runtimes Silver e o schema_drift_engine.
# Resolve criticidade pela policy canônica e NÃO expõe bootstrap automático.

from __future__ import annotations

import importlib.util
import json
import os

import yaml
from pyspark.sql import DataFrame


_ENGINE_PATH = os.path.join(os.path.dirname(__file__), "schema_drift_engine.py")
_spec = importlib.util.spec_from_file_location("varejinho_schema_drift_engine", _ENGINE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Não foi possível carregar schema_drift_engine: {_ENGINE_PATH}")
_engine = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_engine)

SchemaDriftEngine = _engine.SchemaDriftEngine
SchemaDriftViolation = _engine.SchemaDriftViolation


class SilverSchemaDriftRuntime:
    """Runtime canônico de drift para a Silver.

    O runtime só pode avaliar um schema contra um baseline previamente aceito.
    Criação/promoção de baseline continua sendo operação explícita e separada.
    """

    def __init__(self, dbutils, control_root: str, bundle_files_path: str):
        self.dbutils = dbutils
        self.control_root = control_root.rstrip("/")
        self.bundle_files_path = bundle_files_path.rstrip("/")
        self.policy_path = f"{self.bundle_files_path}/contracts/silver/_policy.yaml"
        self.engine = SchemaDriftEngine(
            dbutils=dbutils,
            control_root=self.control_root,
        )
        self.entity_tiers = self._load_entity_tiers()

    def _load_entity_tiers(self):
        if not os.path.isfile(self.policy_path):
            raise SchemaDriftViolation(
                f"Policy de criticidade não encontrada: {self.policy_path}"
            )

        with open(self.policy_path, "r", encoding="utf-8") as f:
            policy = yaml.safe_load(f) or {}

        entity_tiers = {}
        for tier, cfg in (policy.get("tiers", {}) or {}).items():
            for entity in cfg.get("entities", []) or []:
                if entity in entity_tiers:
                    raise SchemaDriftViolation(
                        f"Entidade duplicada na policy de drift: {entity}"
                    )
                entity_tiers[entity] = tier

        if len(entity_tiers) != 37:
            raise SchemaDriftViolation(
                f"Policy de drift esperava 37 entidades; encontrado={len(entity_tiers)}"
            )
        return entity_tiers

    def tier(self, entity: str) -> str:
        tier = self.entity_tiers.get(entity)
        if tier is None:
            raise SchemaDriftViolation(
                f"{entity}: entidade não classificada na policy de drift"
            )
        return tier

    def evaluate(self, entity: str, df: DataFrame):
        accepted, report = self.engine.evaluate(
            entity,
            df,
            tier=self.tier(entity),
        )
        self.log_report(entity, report)
        return accepted, report

    @staticmethod
    def log_report(entity: str, report: dict) -> None:
        summary = {
            "classification": report.get("classification"),
            "action": report.get("action"),
            "baseline_version": report.get("baseline_version"),
            "added_columns": report.get("added_columns", []),
            "removed_columns": report.get("removed_columns", []),
            "type_changes": report.get("type_changes", {}),
            "event_id": report.get("event_id"),
        }
        print(
            f"[SCHEMA_DRIFT] {entity}: "
            + json.dumps(summary, ensure_ascii=False, default=str)
        )
