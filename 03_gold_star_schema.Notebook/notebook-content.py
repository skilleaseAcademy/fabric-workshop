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


staging_tables = [
    "staging_product_category",
    "staging_customers",
    "staging_geolocation",
    "staging_order_items",
    "staging_order_payments",
    "staging_order_reviews",
    "staging_orders",
    "staging_products",
    "staging_sellers"
]

for table in staging_tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

print("\n All staging tables dropped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================
# NOTEBOOK 3 — Gold: Star Schema (the deliverable for Shashank)
# Lakehouse: lh_workshop
# Purpose: Build dim_date, dim_seller, dim_customer, dim_product
#          and the fact_sales table at item grain.
# Runtime: ~3 minutes
# ================================
# Star schema design :
#   - fact_sales at order-item grain (one row per product per order)
#   - Order-level metrics become DAX roll-ups in Power BI
#
#                  dim_date
#                     |
#   dim_seller -- fact_sales -- dim_product
#                     |
#                 dim_customer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, dayofweek, weekofyear, quarter,
    date_format, expr, lit, when, sequence, explode, sum as spark_sum
)
from pyspark.sql import functions as F
 
# All Gold tables built in this notebook use a "staging_" prefix so they
# never collide with the final Warehouse tables. The next step (T-SQL)
# reads from these and creates the real Gold tables in wh_workshop.
STAGE = "staging_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# dim_date ---------------------------------------------------
# Quick Note: a proper date dimension unlocks time-intelligence DAX.
# We generate every date from 2016-01-01 to 2019-12-31 - covers the dataset
# plus a buffer for "same day last year" calculations.
 

print("Building staging_dim_date...")
 
date_range = spark.sql("""
    SELECT explode(sequence(
        to_date('2016-01-01'),
        to_date('2019-12-31'),
        interval 1 day
    )) AS date_key
""")
 
dim_date = (date_range
    .withColumn("year",          year("date_key"))
    .withColumn("quarter",       quarter("date_key"))
    .withColumn("month",         month("date_key"))
    .withColumn("month_name",    date_format("date_key", "MMMM"))
    .withColumn("day_of_month",  dayofmonth("date_key"))
    .withColumn("day_of_week",   dayofweek("date_key"))
    .withColumn("day_name",      date_format("date_key", "EEEE"))
    .withColumn("week_of_year",  weekofyear("date_key"))
    .withColumn("year_month",    date_format("date_key", "yyyy-MM"))
    .withColumn("is_weekend",    when(dayofweek("date_key").isin(1, 7), True).otherwise(False))
)
 
dim_date.write.mode("overwrite").format("delta").saveAsTable(f"{STAGE}dim_date")
print(f"  {STAGE}dim_date: {spark.table(f'{STAGE}dim_date').count():,} rows")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Populate — staging_dim_seller, staging_dim_customer, staging_dim_product
print("Building staging_dim_seller...")
dim_seller = (spark.table("silver_sellers")
    .select(
        col("seller_id").alias("seller_key"),
        col("seller_city"),
        col("seller_state"),
        col("seller_zip_code_prefix")
    )
)
dim_seller.write.mode("overwrite").format("delta").saveAsTable(f"{STAGE}dim_seller")
 
print("Building staging_dim_customer...")
dim_customer = (spark.table("silver_customers")
    .select(
        col("customer_id").alias("customer_key"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state"),
        col("customer_zip_code_prefix")
    )
)
dim_customer.write.mode("overwrite").format("delta").saveAsTable(f"{STAGE}dim_customer")
 
print("Building staging_dim_product...")
dim_product = (spark.table("silver_products")
    .select(
        col("product_id").alias("product_key"),
        col("product_category_name").alias("category_pt"),
        col("product_category_name_english").alias("category_en"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    )
)
dim_product.write.mode("overwrite").format("delta").saveAsTable(f"{STAGE}dim_product")
 
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# staging_fact_sales (the centrepiece) -----------------------
# Grain decision (from Round 4): one row per (order_id, order_item_id)
# Joins to: dim_date, dim_seller, dim_customer, dim_product
 
print("Building staging_fact_sales...")
 
fact_sales = (
    spark.table("silver_order_items").alias("oi")
        .join(spark.table("silver_orders").alias("o"), on="order_id", how="inner")
        .select(
            # Degenerate dimensions
            col("o.order_id"),
            col("oi.order_item_id"),
            # Foreign keys
            col("o.order_purchase_date").alias("order_date_key"),
            col("o.customer_id").alias("customer_key"),
            col("oi.seller_id").alias("seller_key"),
            col("oi.product_id").alias("product_key"),
            # Measures
            col("oi.price").alias("revenue"),
            col("oi.freight_value").alias("freight"),
            col("oi.total_item_value").alias("gross_value"),
            # Late-delivery flag pre-computed at fact level
            col("o.dq_is_late_delivery").alias("is_late_delivery"),
            col("o.dq_currently_overdue").alias("is_currently_overdue"),
            # Shipping SLA - the KPI 3 column
            col("oi.shipping_limit_date"),
            col("o.order_delivered_carrier_date"),
            col("o.order_status"),
            col("o.order_estimated_delivery_date"),
            col("o.order_delivered_customer_date")
        )
        # Exclude canceled orders from revenue facts but flag them
        .withColumn("is_revenue_recognised",
                    when(col("order_status").isin("delivered", "shipped", "invoiced"),
                         True).otherwise(False))
)
 
fact_sales.write.mode("overwrite").format("delta").saveAsTable(f"{STAGE}fact_sales")
print(f"  {STAGE}fact_sales: {spark.table(f'{STAGE}fact_sales').count():,} rows")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the staged schema ---------------------------------
# Talking point: every star schema needs row-count + FK validation
# before you promote it to the Warehouse.
 
print("\nStaged schema validation:")
 
# Row counts
for tbl in [f"{STAGE}fact_sales", f"{STAGE}dim_date", f"{STAGE}dim_seller",
            f"{STAGE}dim_customer", f"{STAGE}dim_product"]:
    print(f"  {tbl:30s}  {spark.table(tbl).count():>10,} rows")
 
# Orphan check - facts pointing to non-existent dimension keys
print("\nOrphan FK check (should all be zero):")
orphans = spark.sql(f"""
    SELECT
      SUM(CASE WHEN s.seller_key   IS NULL THEN 1 ELSE 0 END) AS orphan_seller,
      SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END) AS orphan_customer,
      SUM(CASE WHEN p.product_key  IS NULL THEN 1 ELSE 0 END) AS orphan_product,
      SUM(CASE WHEN d.date_key     IS NULL THEN 1 ELSE 0 END) AS orphan_date
    FROM {STAGE}fact_sales f
    LEFT JOIN {STAGE}dim_seller   s ON f.seller_key   = s.seller_key
    LEFT JOIN {STAGE}dim_customer c ON f.customer_key = c.customer_key
    LEFT JOIN {STAGE}dim_product  p ON f.product_key  = p.product_key
    LEFT JOIN {STAGE}dim_date     d ON f.order_date_key = d.date_key
""")
display(orphans)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optimise Lakehouse staging for fast cross-database reads ---
# V-Order layout + Z-Order on the most-filtered column.
# The Warehouse will read these via three-part naming in the CTAS step;
# optimising here means the cross-database read in step 03b is fast.
 
print("\nOptimising staging Delta tables (V-Order + Z-Order)...")
spark.sql(f"OPTIMIZE {STAGE}fact_sales VORDER")
spark.sql(f"OPTIMIZE {STAGE}fact_sales ZORDER BY (order_date_key, seller_key)")
for dim in ["dim_date", "dim_seller", "dim_customer", "dim_product"]:
    spark.sql(f"OPTIMIZE {STAGE}{dim} VORDER")
print("Staging is ready. Next: run 03b_gold_to_warehouse.sql in wh_workshop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
