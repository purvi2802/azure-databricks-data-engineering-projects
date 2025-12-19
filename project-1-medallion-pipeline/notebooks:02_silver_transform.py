# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df_transactions = spark.read.parquet(
    "/mnt/retail_project/bronze/transaction/"
)

df_products = spark.read.parquet(
    "/mnt/retail_project/bronze/product/"
)

df_stores = spark.read.parquet(
    "/mnt/retail_project/bronze/store/"
)

df_customers = spark.read.parquet(
    "/mnt/retail_project/bronze/customer/manish040596/azure-data-engineer---multi-source/refs/heads/main/"
)


# COMMAND ----------

df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    col("customer_id").cast("int"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("city"),
    col("registration_date")
).dropDuplicates(["customer_id"])


# COMMAND ----------

df_silver = (
    df_transactions
    .join(df_customers, "customer_id")
    .join(df_products, "product_id")
    .join(df_stores, "store_id")
    .withColumn("total_amount", col("quantity") * col("price"))
)

display(df_silver)


# COMMAND ----------

silver_path = "/mnt/retail_project/silver_v2/"

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_path)

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS retail_silver_cleaned
""")

spark.sql("""
CREATE TABLE retail_silver_cleaned
USING DELTA
LOCATION '/mnt/retail_project/silver_v2/'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_silver_cleaned;
# MAGIC