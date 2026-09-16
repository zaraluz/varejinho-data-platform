# quality/contract_engine.py
# Motor de validação de contratos YAML para a camada Silver
# Lê o contrato da tabela, aplica as regras e separa válidos de inválidos
# Nunca bloqueia o pipeline — registros inválidos vão para quarentena

import yaml
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Tuple, Dict

class ContractValidator:

    def __init__(self, contract_path: str):
        """
        Lê o contrato YAML da tabela.
        contract_path: caminho absoluto do arquivo YAML no Workspace ou S3
        """
        with open(contract_path, "r") as f:
            self.contract = yaml.safe_load(f)

        self.table    = self.contract.get("table", "unknown")
        self.columns  = self.contract.get("columns", [])
        self.rules    = self.contract.get("quality_rules", [])

    def validate(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, Dict]:
        """
        Aplica as regras do contrato ao DataFrame.
        Retorna: (df_ok, df_quarentena, relatorio)
        """
        # Começa com todos os registros válidos
        df = df.withColumn("_invalido", F.lit(False))
        df = df.withColumn("_motivo",   F.lit(""))

        for col_cfg in self.columns:
            col_name = col_cfg.get("name")
            nullable = col_cfg.get("nullable", True)
            unique   = col_cfg.get("unique", False)
            min_val  = col_cfg.get("min", None)

            if col_name not in df.columns:
                continue

            # not_null
            if not nullable:
                df = df.withColumn("_invalido",
                    F.when(F.col(col_name).isNull(), F.lit(True))
                    .otherwise(F.col("_invalido")))
                df = df.withColumn("_motivo",
                    F.when(F.col(col_name).isNull(),
                        F.concat(F.col("_motivo"), F.lit(f"|{col_name} é nulo")))
                    .otherwise(F.col("_motivo")))

            # min value
            if min_val is not None:
                try:
                    min_num = float(min_val)
                    df = df.withColumn("_invalido",
                        F.when(F.col(col_name).cast("double") < min_num, F.lit(True))
                        .otherwise(F.col("_invalido")))
                    df = df.withColumn("_motivo",
                        F.when(F.col(col_name).cast("double") < min_num,
                            F.concat(F.col("_motivo"), F.lit(f"|{col_name} < {min_val}")))
                        .otherwise(F.col("_motivo")))
                except (ValueError, TypeError):
                    pass

        # Separa válidos de inválidos
        df_ok   = df.where(~F.col("_invalido")).drop("_invalido", "_motivo")
        df_quar = df.where( F.col("_invalido")).drop("_invalido")

        total      = df.count()
        invalidos  = df_quar.count()
        validos    = total - invalidos

        relatorio = {
            "tabela":      self.table,
            "total":       total,
            "validos":     validos,
            "quarentena":  invalidos,
            "percentual":  round(invalidos / total * 100, 2) if total > 0 else 0
        }

        return df_ok, df_quar, relatorio