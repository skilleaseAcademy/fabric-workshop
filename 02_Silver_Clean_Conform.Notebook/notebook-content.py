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


silver_tables = [
    "silver_product_category",
    "silver_customers",
    "silver_geolocation",
    "silver_order_items",
    "silver_order_payments",
    "silver_order_reviews",
    "silver_orders",
    "silver_products",
    "silver_sellers"
]

for table in silver_tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

print("\n All silver tables dropped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ======================
# # NOTEBOOK 2 — Silver: Clean & Conform
# # Lakehouse: lh_workshop
# # Purpose: Type-cast, deduplicate, handle nulls, parse dates
# # Runtime: ~3 minutes
# # ========================
# # Quick Note:
# #   - Bronze is the raw archive (kept forever)
# #   - Silver is "trusted" - typed, deduplicated, quality-flagged
# #   - We DO NOT drop rows here. We flag them and keep them.
#  


# CELL ********************

from pyspark.sql.functions import (
    col, to_timestamp, to_date, when, lit, current_timestamp,
    coalesce, trim, lower, count, sum as spark_sum
)
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean: orders ---------------------------------------------
# Parse timestamps, derive useful flags, keep all rows (don't filter cancelled)
print("Building silver_orders...")
 
silver_orders = (
    spark.table("bronze_orders")
        .withColumn("order_purchase_timestamp",   to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",          to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
        .withColumn("order_purchase_date",   to_date("order_purchase_timestamp"))
        # Quality flags - what got us here (Quick Note: we DON'T silently drop)
        .withColumn("dq_missing_carrier_event",
                    when(col("order_delivered_customer_date").isNotNull() &
                         col("order_delivered_carrier_date").isNull(), True).otherwise(False))
        .withColumn("dq_is_late_delivery",
                    when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                         True).otherwise(False))
        .withColumn("dq_currently_overdue",
                    when(col("order_delivered_customer_date").isNull() &
                         (col("order_estimated_delivery_date") < lit("2018-10-17")) &
                         (col("order_status") != "canceled"),
                         True).otherwise(False))
        # ^ Note: hardcoded "today" = data max date. In prod use current_timestamp().
        .dropDuplicates(["order_id"])
)
 
silver_orders.write.mode("overwrite").format("delta").saveAsTable("silver_orders")
print(f"  silver_orders: {spark.table('silver_orders').count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean: order_items ---------------------------------------
# Parse the shipping_limit_date - critical for KPI 3 (sellers about to break SLA)
print("Building silver_order_items...")
 
silver_order_items = (
    spark.table("bronze_order_items")
        .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
        .withColumn("price",         col("price").cast("decimal(12,2)"))
        .withColumn("freight_value", col("freight_value").cast("decimal(12,2)"))
        .withColumn("total_item_value", col("price") + col("freight_value"))
        # Composite-key dedup (one order can have multiple items)
        .dropDuplicates(["order_id", "order_item_id"])
)
 
silver_order_items.write.mode("overwrite").format("delta").saveAsTable("silver_order_items")
print(f"  silver_order_items: {spark.table('silver_order_items').count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean: sellers, customers, products ----------------------
# Trim whitespace, lowercase state codes, handle uncategorised products
 
print("Building silver_sellers...")
silver_sellers = (
    spark.table("bronze_sellers")
        .withColumn("seller_city",  trim(lower(col("seller_city"))))
        .withColumn("seller_state", trim(col("seller_state")))
        .dropDuplicates(["seller_id"])
)
silver_sellers.write.mode("overwrite").format("delta").saveAsTable("silver_sellers")
 
print("Building silver_customers...")
silver_customers = (
    spark.table("bronze_customers")
        .withColumn("customer_city",  trim(lower(col("customer_city"))))
        .withColumn("customer_state", trim(col("customer_state")))
        .dropDuplicates(["customer_id"])
)
silver_customers.write.mode("overwrite").format("delta").saveAsTable("silver_customers")
 
print("Building silver_products...")
# Apply the role-play decision: 610 missing categories -> 'Uncategorised' bucket
silver_products = (
    spark.table("bronze_products")
        .withColumn("product_category_name",
                    coalesce(trim(col("product_category_name")), lit("uncategorised")))
        .dropDuplicates(["product_id"])
)
# Join with category translation to get English names
translations = spark.table("bronze_product_category_translation") \
    .drop("_ingested_at").drop("_source_file")

silver_products = (silver_products
    .join(translations, on="product_category_name", how="left")
    .withColumn("product_category_name_english",
                coalesce(col("product_category_name_english"), col("product_category_name")))
)
silver_products.write.mode("overwrite").format("delta").saveAsTable("silver_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quality summary --------------------------------------------
# This is the report wanted - "tell me what's broken"
print("\nData quality summary (silver_orders):")
qa = spark.sql("""
    SELECT
      COUNT(*)                                       AS total_orders,
      SUM(CASE WHEN dq_missing_carrier_event THEN 1 ELSE 0 END)  AS missing_carrier_events,
      SUM(CASE WHEN dq_is_late_delivery     THEN 1 ELSE 0 END)  AS late_deliveries,
      SUM(CASE WHEN dq_currently_overdue    THEN 1 ELSE 0 END)  AS currently_overdue,
      ROUND(100.0 * SUM(CASE WHEN dq_is_late_delivery THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_pct
    FROM silver_orders
    WHERE order_status NOT IN ('canceled', 'unavailable')
""")
display(qa)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5 — Uncategorised product summary -------------------------------
display(spark.sql("""
    SELECT
      CASE WHEN product_category_name = 'uncategorised'
           THEN 'Uncategorised' ELSE 'Has category' END AS bucket,
      COUNT(*) AS product_count
    FROM silver_products
    GROUP BY 1
    ORDER BY 2 DESC
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
