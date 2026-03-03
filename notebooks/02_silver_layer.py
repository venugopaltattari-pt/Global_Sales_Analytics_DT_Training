from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load Bronze tables
orders = spark.table("global_sales.bronze_orders")
products = spark.table("global_sales.bronze_products")
customers = spark.table("global_sales.bronze_customers")
regions = spark.table("global_sales.bronze_regions")

# COMMAND ----------

orders_clean = (
    orders
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("revenue") > 0)
    .withColumn("order_date", to_date(col("order_date")))
)

print("Cleaned Orders Count:", orders_clean.count())

# COMMAND ----------

sales_enriched = (
    orders_clean
    .join(products, "product_id", "left")
    .join(customers, "customer_id", "left")
    .join(regions, "region_id", "left")
)

display(sales_enriched.limit(5))

# COMMAND ----------

orders_clean = sales_enriched.drop("cost_price")
sales_enriched = (
    orders_clean
    .withColumn("year", year("order_date"))
    .withColumn("month", month("order_date"))
    .withColumn("profit_margin", round((col("profit") / col("revenue")) * 100, 2))
    .withColumn(
        "revenue_bucket",
        when(col("revenue") < 100, "Low")
        .when(col("revenue") < 500, "Medium")
        .otherwise("High")
    )
)

# COMMAND ----------

silver_path = "dbfs:/FileStore/global_sales/silver"

sales_enriched.write.format("delta").mode("overwrite").save(f"{silver_path}/sales_enriched")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.silver_sales_enriched
USING DELTA
LOCATION '{silver_path}/sales_enriched'
""")