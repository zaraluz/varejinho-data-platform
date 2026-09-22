# quality/schema_drift_engine.py
# Engine canônico de Schema Drift para a camada Silver.
#
# Princípios:
# - detectar drift != promover novo baseline
# - baseline ausente em runtime -> FAIL CLOSED
# - additive compatível -> registra evento e pode seguir SEM evoluir schema automaticamente
# - removed_column / type_change / mixed breaking -> bloqueia
# - promoção de baseline é operação explícita, auditável e separada

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame


class SchemaDriftViolation(Exception):
    """Drift breaking ou estado de registry inválido que deve bloquear o runtime."""


class SchemaDriftEngine:
    def __init__(self, dbutils, control_root: str, allow_additive: bool = True):
        self.dbutils = dbutils
        self.control_root = control_root.rstrip("/")
        self.registry_root = f"{self.control_root}/schema_registry"
        self.events_root = f"{self.registry_root}/events"
        self.promotions_root = f"{self.registry_root}/promotions"
        self.allow_additive = allow_additive

    @staticmethod
    def schema_of(df: DataFrame) -> Dict[str, str]:
        return {
            field.name: field.dataType.simpleString()
            for field in df.schema.fields
        }

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _hash_schema(schema: Dict[str, str]) -> str:
        payload = json.dumps(schema, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def baseline_path(self, entity: str) -> str:
        return f"{self.registry_root}/{entity}.json"

    def event_path(self, entity: str, event_id: str) -> str:
        return f"{self.events_root}/{entity}/{event_id}.json"

    def promotion_path(self, entity: str, event_id: str) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        return f"{self.promotions_root}/{entity}/{event_id}_{ts}.json"

    def _read_json(self, path: str) -> dict:
        raw = self.dbutils.fs.head(path)
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise SchemaDriftViolation(f"JSON inválido em {path}: esperado objeto")
        return value

    def _exists(self, path: str) -> bool:
        try:
            self.dbutils.fs.head(path)
            return True
        except Exception:
            return False

    def _write_json(self, path: str, payload: dict, overwrite: bool) -> None:
        parent = path.rsplit("/", 1)[0]
        self.dbutils.fs.mkdirs(parent)
        self.dbutils.fs.put(
            path,
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            overwrite=overwrite,
        )

    def load_baseline(self, entity: str) -> Tuple[Dict[str, str], dict]:
        path = self.baseline_path(entity)
        if not self._exists(path):
            raise SchemaDriftViolation(
                f"{entity}: baseline ausente em {path}. "
                "Runtime não cria baseline automaticamente."
            )

        payload = self._read_json(path)

        # Compatibilidade com os 14 baselines legados atuais: {coluna: tipo}.
        if "schema" not in payload:
            schema = {str(k): str(v) for k, v in payload.items()}
            return schema, {
                "format": "legacy",
                "version": 1,
                "schema": schema,
                "schema_hash": self._hash_schema(schema),
            }

        schema = payload.get("schema")
        if not isinstance(schema, dict):
            raise SchemaDriftViolation(
                f"{entity}: baseline envelope inválido; 'schema' não é objeto"
            )
        normalized = {str(k): str(v) for k, v in schema.items()}
        meta = dict(payload)
        meta["schema"] = normalized
        meta.setdefault("version", 1)
        meta.setdefault("schema_hash", self._hash_schema(normalized))
        return normalized, meta

    def bootstrap_baseline(
        self,
        entity: str,
        df: DataFrame,
        approved_by: str,
        reason: str,
    ) -> dict:
        """Criação explícita de baseline. Nunca é chamada implicitamente pelo evaluate()."""
        path = self.baseline_path(entity)
        if self._exists(path):
            raise SchemaDriftViolation(
                f"{entity}: baseline já existe; bootstrap não pode sobrescrever"
            )

        schema = self.schema_of(df)
        payload = {
            "entity": entity,
            "version": 1,
            "schema": schema,
            "schema_hash": self._hash_schema(schema),
            "accepted_at": self._now_iso(),
            "accepted_by": approved_by,
            "reason": reason,
            "source": "explicit_bootstrap",
        }
        self._write_json(path, payload, overwrite=False)
        return payload

    @staticmethod
    def _diff(baseline: Dict[str, str], observed: Dict[str, str]) -> dict:
        added = sorted(set(observed) - set(baseline))
        removed = sorted(set(baseline) - set(observed))
        type_changes = {
            col: {"before": baseline[col], "after": observed[col]}
            for col in sorted(set(baseline) & set(observed))
            if baseline[col] != observed[col]
        }
        return {
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changes,
        }

    @staticmethod
    def _classify(diff: dict) -> str:
        has_added = bool(diff["added_columns"])
        has_removed = bool(diff["removed_columns"])
        has_type = bool(diff["type_changes"])

        if not (has_added or has_removed or has_type):
            return "no_drift"
        if has_added and not has_removed and not has_type:
            return "additive"
        if has_removed and not has_added and not has_type:
            return "removed_column"
        if has_type and not has_added and not has_removed:
            return "type_change"
        return "mixed_breaking"

    def _event_id(
        self,
        entity: str,
        baseline_schema: Dict[str, str],
        observed_schema: Dict[str, str],
    ) -> str:
        raw = "|".join(
            [
                entity,
                self._hash_schema(baseline_schema),
                self._hash_schema(observed_schema),
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def _persist_event(self, event: dict) -> str:
        path = self.event_path(event["entity"], event["event_id"])
        if not self._exists(path):
            self._write_json(path, event, overwrite=False)
        return path

    def evaluate(
        self,
        entity: str,
        df: DataFrame,
        tier: str,
        allow_additive: Optional[bool] = None,
    ) -> Tuple[DataFrame, dict]:
        """
        Compara schema observado com baseline aceito.

        Retorna um DataFrame compatível com o baseline aceito. Em additive permitido,
        colunas novas são deliberadamente projetadas para fora: o pipeline continua,
        mas o schema Silver NÃO evolui até promoção explícita.
        """
        baseline_schema, baseline_meta = self.load_baseline(entity)
        observed_schema = self.schema_of(df)
        diff = self._diff(baseline_schema, observed_schema)
        classification = self._classify(diff)
        effective_allow_additive = (
            self.allow_additive if allow_additive is None else allow_additive
        )

        report = {
            "entity": entity,
            "tier": tier,
            "classification": classification,
            "drift_detected": classification != "no_drift",
            "baseline_version": baseline_meta.get("version", 1),
            "baseline_schema_hash": self._hash_schema(baseline_schema),
            "observed_schema_hash": self._hash_schema(observed_schema),
            **diff,
        }

        if classification == "no_drift":
            report.update({"action": "ALLOW", "event_id": None, "event_path": None})
            return df.select(*baseline_schema.keys()), report

        event_id = self._event_id(entity, baseline_schema, observed_schema)
        is_additive_allowed = classification == "additive" and effective_allow_additive
        action = "ALLOW_WITH_BASELINE_PROJECTION" if is_additive_allowed else "BLOCK"

        event = {
            "event_id": event_id,
            "entity": entity,
            "tier": tier,
            "detected_at": self._now_iso(),
            "classification": classification,
            "action": action,
            "baseline_version": baseline_meta.get("version", 1),
            "baseline_schema_hash": self._hash_schema(baseline_schema),
            "observed_schema_hash": self._hash_schema(observed_schema),
            "baseline_schema": baseline_schema,
            "observed_schema": observed_schema,
            **diff,
        }
        event_path = self._persist_event(event)
        report.update({"action": action, "event_id": event_id, "event_path": event_path})

        if is_additive_allowed:
            # Não deixa uma coluna nova entrar em Silver só porque foi observada.
            return df.select(*baseline_schema.keys()), report

        raise SchemaDriftViolation(
            f"{entity}: schema drift bloqueado | classification={classification} "
            f"| event_id={event_id} | added={diff['added_columns']} "
            f"| removed={diff['removed_columns']} | type_changes={diff['type_changes']}"
        )

    def promote_event(
        self,
        entity: str,
        event_id: str,
        approved_by: str,
        reason: str,
        allow_breaking: bool = False,
    ) -> dict:
        """Promove explicitamente o schema observado em um evento para novo baseline."""
        event_path = self.event_path(entity, event_id)
        if not self._exists(event_path):
            raise SchemaDriftViolation(
                f"{entity}: evento não encontrado para promoção: {event_id}"
            )

        event = self._read_json(event_path)
        classification = event.get("classification")
        if classification != "additive" and not allow_breaking:
            raise SchemaDriftViolation(
                f"{entity}: promoção breaking exige allow_breaking=True; "
                f"classification={classification}"
            )

        current_schema, current_meta = self.load_baseline(entity)
        current_hash = self._hash_schema(current_schema)
        if current_hash != event.get("baseline_schema_hash"):
            raise SchemaDriftViolation(
                f"{entity}: baseline mudou desde a detecção; promoção requer nova avaliação"
            )

        observed = event.get("observed_schema")
        if not isinstance(observed, dict):
            raise SchemaDriftViolation(
                f"{entity}: evento {event_id} sem observed_schema válido"
            )
        observed = {str(k): str(v) for k, v in observed.items()}

        new_version = int(current_meta.get("version", 1)) + 1
        baseline_payload = {
            "entity": entity,
            "version": new_version,
            "schema": observed,
            "schema_hash": self._hash_schema(observed),
            "accepted_at": self._now_iso(),
            "accepted_by": approved_by,
            "reason": reason,
            "source": "approved_drift_event",
            "source_event_id": event_id,
        }
        self._write_json(self.baseline_path(entity), baseline_payload, overwrite=True)

        promotion = {
            "entity": entity,
            "event_id": event_id,
            "promoted_at": self._now_iso(),
            "approved_by": approved_by,
            "reason": reason,
            "classification": classification,
            "from_version": current_meta.get("version", 1),
            "to_version": new_version,
            "new_schema_hash": baseline_payload["schema_hash"],
        }
        promotion_path = self.promotion_path(entity, event_id)
        self._write_json(promotion_path, promotion, overwrite=False)

        return {
            "baseline": baseline_payload,
            "promotion": promotion,
            "promotion_path": promotion_path,
        }
