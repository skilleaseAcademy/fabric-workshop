# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b9da4b7e-4578-48f5-b00c-e94b7047e654",
# META       "default_lakehouse_name": "lh_workshop",
# META       "default_lakehouse_workspace_id": "f1fd358a-fa70-4736-8143-90abd6b0e537",
# META       "known_lakehouses": [
# META         {
# META           "id": "b9da4b7e-4578-48f5-b00c-e94b7047e654"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


bronze_tables = [
    "bronze_product_category",
    "bronze_customers",
    "bronze_geolocation",
    "bronze_order_items",
    "bronze_order_payments",
    "bronze_order_reviews",
    "bronze_orders",
    "bronze_products",
    "bronze_sellers"
]

for table in bronze_tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

print("\n All Bronze tables dropped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # =============================
# # NOTEBOOK 1 — Bronze Ingestion
# # Lakehouse: lh_workshop
# # Cluster: default Spark pool (any size)
# # Purpose: Land the 9 raw Olist CSVs as Delta tables in the Bronze layer
# # Runtime: ~2 minutes on a small Starter Pool
# # =============================

# CELL ********************

# Imports and config -----------------------------------------
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp
from pyspark.sql.types import StringType
import time
 
# Bronze tables get a "bronze_" prefix and an ingestion timestamp.
# Raw data is preserved AS-IS - no cleansing happens here.
BRONZE_PREFIX = "bronze_"
 
# The 9 source files - already uploaded to Files/raw/ by the prereq step
RAW_BASE = "Files/raw/"
 
# A small dictionary mapping source file -> target Bronze table name.
# Keeping it explicit makes the demo easy to narrate.
SOURCES = {
    "olist_orders_dataset.csv":             "orders",
    "olist_order_items_dataset.csv":        "order_items",
    "olist_order_payments_dataset.csv":     "order_payments",
    "olist_order_reviews_dataset.csv":      "order_reviews",
    "olist_customers_dataset.csv":          "customers",
    "olist_sellers_dataset.csv":            "sellers",
    "olist_products_dataset.csv":           "products",
    "olist_geolocation_dataset.csv":        "geolocation",
    "product_category_name_translation.csv":"product_category_translation",
}
 
print(f"Bronze ingestion - {len(SOURCES)} files to process.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ingest each CSV to a Bronze Delta table --------------------
# Note: notice we're reading CSV but writing Delta. Delta is what unlocks DirectLake, time-travel, and ACID guarantees later.
 
for src_file, target_table in SOURCES.items():
    src_path = f"{RAW_BASE}{src_file}"
    target = f"{BRONZE_PREFIX}{target_table}"
    t0 = time.time()
 
    df = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .option("escape", "\"")
                .csv(src_path))
 
    # Add audit columns - every Bronze row knows when it landed
    df = (df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", lit(src_file)))
 
    # Write as managed Delta table to the Lakehouse
    (df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(target))
 
    rows = spark.table(target).count()
    elapsed = round(time.time() - t0, 1)
    print(f"  {target:35s}  {rows:>9,} rows  ({elapsed}s)")
 
print("\nBronze layer complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  Quick sanity check -----------------------------------------
# Confirm row counts match expected (Note: data integrity check)
EXPECTED = {
    "bronze_orders":          99_441,   # ± 1 row tolerance
    "bronze_order_items":     112_650,
    "bronze_customers":       99_441,
    "bronze_sellers":           3_095,
    "bronze_products":         32_951,
    "bronze_order_payments":  103_886,
    "bronze_order_reviews":    99_223,
}
 
print("Sanity check (expected vs actual):")
for tbl, expected in EXPECTED.items():
    actual = spark.table(tbl).count()
    flag = "OK " if abs(actual - expected) <= 2 else "WARN"
    print(f"  {flag} {tbl:30s} expected ~{expected:>9,}  actual {actual:>9,}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect one Bronze table -----------------------------------
# Display what's actually in Bronze. 
 
display(spark.table("bronze_orders").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
