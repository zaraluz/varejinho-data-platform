# Databricks notebook source
# pipeline/gold/profile_contas_pagar_reconciliation.py
# Release Gate diagnostic — read-only reconciliation for fato_contas_pagar.
#
# Goal:
# prove whether Gold row loss is exactly explained by orphan
# pagarfornecedorparcela rows or by a downstream Gold defect.

from pyspark.sql import functions as F


def job_param(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value else default
    except Exception:
        return default


CATALOG = job_param("catalog", "varejinho_dev")
BRONZE_SOURCE_CATALOG = job_param("bronze_source_catalog", "varejinho")

if not CATALOG.endswith("_dev"):
    raise Exception(
        f"Contas pagar reconciliation is dev-only during hardening. Received: {CATALOG}"
    )

PP = f"{CATALOG}.silver.pagarfornecedorparcela"
PF = f"{CATALOG}.silver.pagarfornecedor"
GOLD = f"{CATALOG}.gold.fato_contas_pagar"
BRONZE_PF = f"{BRONZE_SOURCE_CATALOG}.bronze.pagarfornecedor"

for table in [PP, PF, GOLD, BRONZE_PF]:
    if not spark.catalog.tableExists(table):
        raise Exception(f"Required table missing: {table}")

pp = spark.table(PP).alias("pp")
pf = spark.table(PF).alias("pf")
gold = spark.table(GOLD).alias("g")
bronze_pf = spark.table(BRONZE_PF).alias("bpf")

silver_parcelas = pp.count()
silver_headers = pf.count()
gold_rows = gold.count()

parent_dupes = (
    pf.groupBy("id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

eligible = (
    pp.join(
        pf,
        F.col("pp.id_pagarfornecedor") == F.col("pf.id"),
        "inner",
    )
    .select(
        F.col("pp.id").alias("id_parcela"),
        F.col("pp.id_pagarfornecedor").alias("id_pagarfornecedor"),
        F.col("pp.ingestion_date").alias("parcela_ingestion_date"),
    )
)

eligible_rows = eligible.count()
eligible_dup_ids = (
    eligible.groupBy("id_parcela")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

orphans = (
    pp.join(
        pf.select(F.col("id").alias("_parent_id")),
        F.col("pp.id_pagarfornecedor") == F.col("_parent_id"),
        "left_anti",
    )
)

orphan_rows = orphans.count()
orphan_parent_ids = orphans.select("id_pagarfornecedor").distinct().count()
null_parent_fk = pp.filter(F.col("id_pagarfornecedor").isNull()).count()

bronze_parent_ids = (
    bronze_pf
    .select(F.col("id").alias("_bronze_parent_id"))
    .filter(F.col("_bronze_parent_id").isNotNull())
    .distinct()
)

orphans_with_parent_in_bronze = (
    orphans.join(
        bronze_parent_ids,
        F.col("id_pagarfornecedor") == F.col("_bronze_parent_id"),
        "inner",
    )
)
orphan_parent_present_bronze_rows = orphans_with_parent_in_bronze.count()
orphan_parent_present_bronze_ids = (
    orphans_with_parent_in_bronze
    .select("id_pagarfornecedor")
    .distinct()
    .count()
)

orphans_absent_bronze = (
    orphans.join(
        bronze_parent_ids,
        F.col("id_pagarfornecedor") == F.col("_bronze_parent_id"),
        "left_anti",
    )
)
orphan_parent_absent_bronze_rows = orphans_absent_bronze.count()
orphan_parent_absent_bronze_ids = (
    orphans_absent_bronze
    .select("id_pagarfornecedor")
    .distinct()
    .count()
)

eligible_ids = eligible.select("id_parcela").distinct()
gold_ids = gold.select("id_parcela").distinct()

missing_gold = eligible_ids.join(gold_ids, on="id_parcela", how="left_anti")
extra_gold = gold_ids.join(eligible_ids, on="id_parcela", how="left_anti")

missing_gold_rows = missing_gold.count()
extra_gold_rows = extra_gold.count()

print("\n=== CONTAS PAGAR RECONCILIATION [READ ONLY] ===")
print(f"Silver parcelas:              {silver_parcelas:,}")
print(f"Silver headers:               {silver_headers:,}")
print(f"Eligible inner-join rows:     {eligible_rows:,}")
print(f"Gold rows:                    {gold_rows:,}")
print(f"Raw Silver-Gold difference:   {silver_parcelas - gold_rows:,}")
print(f"Orphan parcel rows:           {orphan_rows:,}")
print(f"Distinct missing parent ids:  {orphan_parent_ids:,}")
print(f"Null parent FK rows:          {null_parent_fk:,}")
print(f"Parent duplicate ids Silver:  {parent_dupes:,}")
print(f"Eligible duplicate parcels:   {eligible_dup_ids:,}")
print()
print(f"Orphans: parent exists Bronze rows: {orphan_parent_present_bronze_rows:,}")
print(f"Orphans: parent exists Bronze ids:  {orphan_parent_present_bronze_ids:,}")
print(f"Orphans: parent absent Bronze rows: {orphan_parent_absent_bronze_rows:,}")
print(f"Orphans: parent absent Bronze ids:  {orphan_parent_absent_bronze_ids:,}")
print()
print(f"Eligible source ids missing Gold:   {missing_gold_rows:,}")
print(f"Gold ids not eligible from Silver:  {extra_gold_rows:,}")

explained_exactly = (
    silver_parcelas - orphan_rows == eligible_rows
    and eligible_rows == gold_rows
    and missing_gold_rows == 0
    and extra_gold_rows == 0
    and parent_dupes == 0
    and eligible_dup_ids == 0
)

print(
    "\nRESULT: "
    + (
        "✅ GOLD LOSS IS EXACTLY EXPLAINED BY ORPHAN PARCELS"
        if explained_exactly
        else "❌ ADDITIONAL DIVERGENCE EXISTS"
    )
)

print("\n--- Orphan rows by parcela ingestion_date ---")
(
    orphans.groupBy("ingestion_date")
    .count()
    .orderBy(F.col("ingestion_date").desc())
    .show(50, truncate=False)
)

print("\n--- Sample orphan parent IDs that EXIST in Bronze but not Silver ---")
(
    orphans_with_parent_in_bronze
    .select("id_pagarfornecedor")
    .distinct()
    .limit(20)
    .show(truncate=False)
)

print("\n--- Sample orphan parent IDs ABSENT from Bronze ---")
(
    orphans_absent_bronze
    .select("id_pagarfornecedor")
    .distinct()
    .limit(20)
    .show(truncate=False)
)

if missing_gold_rows:
    print("\n--- Eligible parcel IDs missing from Gold ---")
    missing_gold.limit(20).show(truncate=False)

if extra_gold_rows:
    print("\n--- Gold parcel IDs without eligible Silver source ---")
    extra_gold.limit(20).show(truncate=False)

print("\n✅ Diagnostic completed. No tables were modified.")
