# Databricks notebook source
df_raw = spark.read.option("header", True).csv("dbfs:/project2/raw/")
display(df_raw)


# COMMAND ----------

from pyspark.sql.functions import trim, col

df_clean = (
    df_raw
    .withColumn("name", trim(col("name")))
    .withColumn("age", col("age").cast("int"))
    .withColumn("salary", col("salary").cast("double"))
)

display(df_clean)


# COMMAND ----------

df_clean.write.mode("overwrite").parquet("dbfs:/project2/clean/")
print("Clean data written to dbfs:/project2/clean/")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/project2/clean"))
