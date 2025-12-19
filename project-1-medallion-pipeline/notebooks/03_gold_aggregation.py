# Databricks notebook source
silver_df = spark.read.format("delta").load(
    "/mnt/retail_project/silver_v2/"
)

display(silver_df)


# COMMAND ----------

from pyspark.sql.functions import sum

gold_df = (
    silver_df
    .groupBy(
        "transaction_date",
        "product_id",
        "product_name",
        "category",
        "store_id",
        "store_name",
        "location"
    )
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        sum("total_amount").alias("total_sales")
    )
)

display(gold_df)


# COMMAND ----------

gold_path = "/mnt/retail_project/gold_v2/"


gold_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(gold_path)


# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS retail_gold_sales_summary
""")

spark.sql("""
CREATE TABLE retail_gold_sales_summary
USING DELTA
LOCATION '/mnt/retail_project/gold_v2/'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_gold_sales_summary;
# MAGIC
