# Databricks notebook source
df_transactions = spark.read.parquet("/mnt/retail_project/bronze/transaction/")
df_products = spark.read.parquet("/mnt/retail_project/bronze/product/")
df_stores = spark.read.parquet("/mnt/retail_project/bronze/store/")
df_customers = spark.read.parquet("/mnt/retail_project/bronze/customer/")

display(df_transactions)


# COMMAND ----------

dbutils.fs.ls("/mnt/retail_project/bronze/")


# COMMAND ----------

# Transactions
df_transactions = spark.read.parquet(
    "/mnt/retail_project/bronze/transaction/"
)

# Products
df_products = spark.read.parquet(
    "/mnt/retail_project/bronze/product/"
)

# Stores
df_stores = spark.read.parquet(
    "/mnt/retail_project/bronze/store/"
)

# Customers (IMPORTANT: deep path)
df_customers = spark.read.parquet(
    "/mnt/retail_project/bronze/customer/manish040596/azure-data-engineer---multi-source/refs/heads/main/"
)

display(df_transactions)
display(df_products)
display(df_stores)
display(df_customers)
