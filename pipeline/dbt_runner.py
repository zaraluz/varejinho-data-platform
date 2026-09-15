# pipeline/dbt_runner.py
# Roda dbt test no Databricks via subprocess
# Chamado pelo DAB após o pipeline diário

import subprocess
import sys

DBT_PROJECT_DIR = "/Workspace/Users/zarallouise@gmail.com/varejinho-data-platform/dbt"

result = subprocess.run(
    ["dbt", "test", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
    capture_output=True,
    text=True
)

print(result.stdout)
print(result.stderr)

if result.returncode != 0:
    raise Exception(f"dbt test falhou com código {result.returncode}")