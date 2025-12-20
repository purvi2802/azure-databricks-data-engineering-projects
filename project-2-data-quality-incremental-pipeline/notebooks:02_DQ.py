# Databricks notebook source
df_clean = spark.read.parquet("dbfs:/project2/clean/")
display(df_clean)


# COMMAND ----------

df_null_age = df_clean.filter("age IS NULL")
df_null_salary = df_clean.filter("salary IS NULL")

display(df_null_age)
display(df_null_salary)


# COMMAND ----------

df_null_age.write.mode("overwrite").csv("dbfs:/project2/rejected/null_age")
df_null_salary.write.mode("overwrite").csv("dbfs:/project2/rejected/null_salary")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/project2/rejected"))


# COMMAND ----------

df_invalid_age = df_clean.filter("age < 18 OR age > 90")
display(df_invalid_age)


# COMMAND ----------

df_invalid_age.write.mode("overwrite").csv("dbfs:/project2/rejected/invalid_age")


# COMMAND ----------

df_clean.describe().write.mode("overwrite").csv("dbfs:/project2/dq_reports/summary")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/project2/dq_reports/summary"))
