# Databricks notebook source
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

np.random.seed(42)

# PARAMETERS
NUM_ORDERS = 100_000
NUM_PRODUCTS = 500
NUM_CUSTOMERS = 5000
NUM_REGIONS = 10

base_path = "/dbfs/FileStore/global_sales"

# REGIONS
regions = pd.DataFrame({
    "region_id": range(1, NUM_REGIONS + 1),
    "region_name": [
        "North America", "Europe", "Asia", "South America",
        "Africa", "Australia", "Middle East",
        "Central America", "East Asia", "West Europe"
    ],
    "market": np.random.choice(["Developed", "Emerging"], NUM_REGIONS)
})

# PRODUCTS
categories = ["Electronics", "Furniture", "Clothing", "Sports", "Food"]
sub_categories = ["Premium", "Standard", "Budget"]

products = pd.DataFrame({
    "product_id": range(1, NUM_PRODUCTS + 1),
    "product_name": [f"Product_{i}" for i in range(1, NUM_PRODUCTS + 1)],
    "category": np.random.choice(categories, NUM_PRODUCTS),
    "sub_category": np.random.choice(sub_categories, NUM_PRODUCTS),
    "cost_price": np.random.uniform(10, 500, NUM_PRODUCTS).round(2)
})

# CUSTOMERS
customers = pd.DataFrame({
    "customer_id": range(1, NUM_CUSTOMERS + 1),
    "customer_name": [f"Customer_{i}" for i in range(1, NUM_CUSTOMERS + 1)],
    "segment": np.random.choice(["Consumer", "Corporate", "Home Office"], NUM_CUSTOMERS),
    "country": np.random.choice(["USA", "India", "Germany", "Brazil", "UK"], NUM_CUSTOMERS)
})

# ORDERS
start_date = datetime(2022, 1, 1)

orders = pd.DataFrame({
    "order_id": range(1, NUM_ORDERS + 1),
    "order_date": [start_date + timedelta(days=random.randint(0, 730)) for _ in range(NUM_ORDERS)],
    "customer_id": np.random.randint(1, NUM_CUSTOMERS + 1, NUM_ORDERS),
    "product_id": np.random.randint(1, NUM_PRODUCTS + 1, NUM_ORDERS),
    "region_id": np.random.randint(1, NUM_REGIONS + 1, NUM_ORDERS),
    "quantity": np.random.randint(1, 10, NUM_ORDERS),
    "unit_price": np.random.uniform(20, 800, NUM_ORDERS).round(2),
    "discount": np.random.uniform(0, 0.3, NUM_ORDERS).round(2)
})

# Realistic revenue + profit calculation
orders["revenue"] = (orders["quantity"] * orders["unit_price"] * (1 - orders["discount"])).round(2)

cost_lookup = dict(zip(products.product_id, products.cost_price))
orders["cost_price"] = orders["product_id"].map(cost_lookup)

orders["profit"] = (
    orders["revenue"] - (orders["quantity"] * orders["cost_price"])
).round(2)

# SAVE FILES
regions.to_csv(f"{base_path}/regions.csv", index=False)
products.to_csv(f"{base_path}/products.csv", index=False)
customers.to_csv(f"{base_path}/customers.csv", index=False)
orders.to_csv(f"{base_path}/orders.csv", index=False)

print("Synthetic dataset created successfully.")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/global_sales"))

# COMMAND ----------

