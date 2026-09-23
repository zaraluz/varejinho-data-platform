# Databricks notebook source
# validation/gold/guard_temporal_apply.py
# Gate Gold Temporal — proteção explícita de hardening.

def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"gold_temporal_apply é dev-only durante hardening. Recebido: {CATALOG}"
    )

print(f"✅ Gold temporal hardening autorizado em {CATALOG}.")
