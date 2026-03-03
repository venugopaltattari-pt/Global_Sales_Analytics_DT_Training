# Bronze Layer Setup

raw_path = "dbfs:/FileStore/global_sales"
bronze_path = "dbfs:/FileStore/global_sales/bronze"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

dbutils.fs.mkdirs(bronze_path)


# Read raw CSV files
orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/orders.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/products.csv")
)

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/customers.csv")
)

regions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/regions.csv")
)

# COMMAND ----------

print("Orders Count:", orders_df.count())
print("Products Count:", products_df.count())
print("Customers Count:", customers_df.count())
print("Regions Count:", regions_df.count())

# COMMAND ----------

# Write as Delta tables

orders_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/orders")
products_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/products")
customers_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/customers")
regions_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/regions")

print("Bronze tables created.")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS global_sales")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.bronze_orders
USING DELTA
LOCATION '{bronze_path}/orders'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.bronze_products
USING DELTA
LOCATION '{bronze_path}/products'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.bronze_customers
USING DELTA
LOCATION '{bronze_path}/customers'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS global_sales.bronze_regions
USING DELTA
LOCATION '{bronze_path}/regions'
""")

# COMMAND ----------

spark.sql("SHOW TABLES IN global_sales").show()