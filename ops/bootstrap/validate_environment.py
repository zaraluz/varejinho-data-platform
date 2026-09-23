# Databricks notebook source
# ops/bootstrap/validate_environment.py
# Valida o isolamento lógico do ambiente antes de liberar transformações destrutivas.
# Em dev, a Bronze deve ser composta por views sobre a Bronze raw de prod,
# enquanto Silver/Gold/Control pertencem exclusivamente ao catálogo dev.

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")
IS_DEV = CATALOG != BRONZE_SOURCE_CATALOG

resultados = []


def check(nome: str, passou: bool, detalhe: str = "") -> None:
    status = "✅" if passou else "❌"
    resultados.append(f"{status} {nome} {detalhe}".rstrip())


# 1) Catálogo e schemas existem.
for schema in ["bronze", "silver", "gold", "control"]:
    try:
        spark.sql(f"DESCRIBE SCHEMA {CATALOG}.{schema}").collect()
        check(f"schema {CATALOG}.{schema} existe", True)
    except Exception as e:
        check(f"schema {CATALOG}.{schema} existe", False, str(e)[:180])


if IS_DEV:
    # 2) Bronze dev deve conter somente views, nunca segundas external tables
    # apontando para os mesmos paths físicos da Bronze raw.
    objetos = spark.sql(f"""
        SELECT table_name, table_type
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema = 'bronze'
    """).collect()

    tipos = {r["table_name"]: r["table_type"] for r in objetos}
    nao_views = {nome: tipo for nome, tipo in tipos.items() if tipo != "VIEW"}

    check(
        "Bronze dev possui 37 objetos",
        len(tipos) == 37,
        f"({len(tipos)} encontrados)",
    )
    check(
        "Bronze dev usa somente VIEW",
        len(nao_views) == 0,
        f"(não-views: {nao_views})" if nao_views else "",
    )

    # 3) Prova simples de equivalência: a view de venda deve enxergar exatamente
    # o mesmo número de linhas da external table raw que a alimenta.
    try:
        source_count = spark.table(f"{BRONZE_SOURCE_CATALOG}.bronze.venda").count()
        dev_count = spark.table(f"{CATALOG}.bronze.venda").count()
        check(
            "Bronze dev.venda = Bronze raw.venda",
            source_count == dev_count,
            f"(raw: {source_count:,} | dev: {dev_count:,})",
        )
    except Exception as e:
        check("Bronze dev.venda = Bronze raw.venda", False, str(e)[:180])

    # 4) Canary de escrita: cria uma tabela temporária somente na Silver dev,
    # verifica que nada apareceu na Silver raw/prod e remove a canary em seguida.
    canary = "_env_isolation_canary"
    dev_canary = f"{CATALOG}.silver.{canary}"
    source_canary = f"{BRONZE_SOURCE_CATALOG}.silver.{canary}"

    source_existed_before = spark.catalog.tableExists(source_canary)
    try:
        (spark.range(1)
            .withColumn("environment", F.lit(CATALOG))
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(dev_canary))

        dev_exists = spark.catalog.tableExists(dev_canary)
        source_exists_after = spark.catalog.tableExists(source_canary)

        check("canary foi escrita na Silver dev", dev_exists)
        check(
            "escrita dev não criou objeto na Silver prod",
            source_exists_after == source_existed_before,
            f"(prod antes={source_existed_before}, depois={source_exists_after})",
        )
    except Exception as e:
        check("canary de isolamento Silver", False, str(e)[:180])
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {dev_canary}")

else:
    # Em prod, a Bronze deve continuar registrada como tabela externa, não view.
    objetos = spark.sql(f"""
        SELECT table_name, table_type
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema = 'bronze'
    """).collect()
    tipos = {r["table_name"]: r["table_type"] for r in objetos}
    views = [nome for nome, tipo in tipos.items() if tipo == "VIEW"]
    check("Bronze prod não usa views de ambiente", len(views) == 0, f"(views: {views})" if views else "")


print("\n=== ENVIRONMENT ISOLATION GATE ===\n")
for r in resultados:
    print(r)

falhas = [r for r in resultados if r.startswith("❌")]
print(f"\n{len(resultados) - len(falhas)}/{len(resultados)} checks passaram | {len(falhas)} falharam")

if falhas:
    raise Exception("Environment Isolation Gate falhou:\n" + "\n".join(falhas))
