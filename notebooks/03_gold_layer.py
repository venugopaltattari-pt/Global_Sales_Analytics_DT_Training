# Databricks notebook source
from pyspark.sql.functions import *

sales = spark.table("global_sales.silver_sales_enriched")

# COMMAND ----------

sales_summary = sales.agg(
    sum("revenue").alias("total_revenue"),
    sum("profit").alias("total_profit"),
    count("order_id").alias("total_orders"),
    avg("revenue").alias("avg_order_value")
).withColumn(
    "overall_profit_margin_pct",
    round((col("total_profit") / col("total_revenue")) * 100, 2)
)

display(sales_summary)

# COMMAND ----------

region_performance = (
    sales.groupBy("region_name", "market")
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("profit").alias("total_profit"),
        count("order_id").alias("total_orders")
    )
    .withColumn(
        "profit_margin_pct",
        round((col("total_profit") / col("total_revenue")) * 100, 2)
    )
    .orderBy(desc("total_revenue"))
)

display(region_performance)

# COMMAND ----------

product_performance = (
    sales.groupBy("category", "sub_category")
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("profit").alias("total_profit"),
        sum("quantity").alias("total_quantity_sold")
    )
    .withColumn(
        "profit_margin_pct",
        round((col("total_profit") / col("total_revenue")) * 100, 2)
    )
    .orderBy(desc("total_revenue"))
)

display(product_performance)

# COMMAND ----------

customer_metrics = (
    sales.groupBy("segment")
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("profit").alias("total_profit"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn(
        "profit_margin_pct",
        round((col("total_profit") / col("total_revenue")) * 100, 2)
    )
)

display(customer_metrics)

# COMMAND ----------

gold_path = "dbfs:/FileStore/global_sales/gold"

sales_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/sales_summary")
region_performance.write.format("delta").mode("overwrite").save(f"{gold_path}/region_performance")
product_performance.write.format("delta").mode("overwrite").save(f"{gold_path}/product_performance")
customer_metrics.write.format("delta").mode("overwrite").save(f"{gold_path}/customer_metrics")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.gold_sales_summary
USING DELTA
LOCATION '{gold_path}/sales_summary'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.gold_region_performance
USING DELTA
LOCATION '{gold_path}/region_performance'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.gold_product_performance
USING DELTA
LOCATION '{gold_path}/product_performance'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.gold_customer_metrics
USING DELTA
LOCATION '{gold_path}/customer_metrics'
""")

# COMMAND ----------

spark.sql("OPTIMIZE global_sales.gold_region_performance")
spark.sql("OPTIMIZE global_sales.gold_product_performance")
spark.sql("VACUUM global_sales.gold_region_performance RETAIN 168 HOURS")

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.partitionBy("region_name").orderBy(desc("revenue"))

top_products_by_region = (
    sales
    .groupBy("region_name", "product_name")
    .agg(sum("revenue").alias("revenue"))
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 5)
)

display(top_products_by_region)

# COMMAND ----------

monthly_trend = (
    sales.groupBy("year", "month")
    .agg(sum("revenue").alias("monthly_revenue"))
    .orderBy("year", "month")
)

display(monthly_trend)

# COMMAND ----------

display(region_performance)

# COMMAND ----------

product_performance_sorted = product_performance.orderBy(desc("total_revenue"))
display(product_performance_sorted)

# COMMAND ----------

display(customer_metrics)