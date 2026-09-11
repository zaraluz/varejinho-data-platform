# databricks/silver/schema_drift.py
import json
from datetime import datetime

REGISTRY_PATH = "s3://varejinho-lake/_control/schema_registry"


def detectar_drift(tabela: str, df, dbutils, spark) -> dict:
    """
    Compara o schema atual do DataFrame com o último registrado.
    Alerta se houver colunas novas, removidas ou tipos alterados.
    Nunca bloqueia o pipeline — Bronze já ingeriu, Silver segue.
    """
    schema_atual = {
        f.name: f.dataType.simpleString()
        for f in df.schema.fields
    }

    registry_file = f"{REGISTRY_PATH}/{tabela}.json"

    # Primeira execução — grava baseline e retorna
    try:
        conteudo = dbutils.fs.head(registry_file)
        schema_anterior = json.loads(conteudo)
    except Exception:
        dbutils.fs.put(
            registry_file,
            json.dumps(schema_atual),
            overwrite=True
        )
        return {"houve_drift": False, "baseline_criado": True, "tabela": tabela}

    # Compara schemas
    novas     = sorted(set(schema_atual) - set(schema_anterior))
    removidas = sorted(set(schema_anterior) - set(schema_atual))
    alteradas = {
        col: {"antes": schema_anterior[col], "depois": schema_atual[col]}
        for col in set(schema_atual) & set(schema_anterior)
        if schema_anterior[col] != schema_atual[col]
    }

    drift = {
        "tabela":            tabela,
        "colunas_novas":     novas,
        "colunas_removidas": removidas,
        "tipos_alterados":   alteradas,
        "houve_drift":       bool(novas or removidas or alteradas),
        "detectado_em":      str(datetime.now()),
    }

    if drift["houve_drift"]:
        # Registra o drift no log
        log_file = f"{REGISTRY_PATH}/drift_log/{tabela}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        dbutils.fs.put(log_file, json.dumps(drift, indent=2), overwrite=True)
        print(f"[DRIFT DETECTADO] {tabela}: {drift}")
        # Aqui entraria o alerta por e-mail — mesmo mecanismo do Pentaho

    # Atualiza o registry com o schema atual
    dbutils.fs.put(registry_file, json.dumps(schema_atual), overwrite=True)

    return drift