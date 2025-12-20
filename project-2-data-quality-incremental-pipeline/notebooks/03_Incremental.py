# Databricks notebook source
# Create state folder if not exists
dbutils.fs.mkdirs("dbfs:/project2/state")

# Create empty state file
dbutils.fs.put("dbfs:/project2/state/last_file.txt", "", True)

print("State file created!")


# COMMAND ----------

last_file = dbutils.fs.head("dbfs:/project2/state/last_file.txt")
print("Last processed file:", last_file)


# COMMAND ----------

files = [f.name for f in dbutils.fs.ls("dbfs:/project2/raw")]
files


# COMMAND ----------

if last_file.strip() == "":
    new_files = files   # first run → process ALL
else:
    idx = files.index(last_file)
    new_files = files[idx+1:]
    
new_files


# COMMAND ----------

for file in new_files:
    print("Processing:", file)
    
    df_new = spark.read.option("header", True).csv(f"dbfs:/project2/raw/{file}")
    
    # Write to incremental folder (append mode)
    df_new.write.mode("append").parquet("dbfs:/project2/incremental")
    
    # Update the state file
    dbutils.fs.put("dbfs:/project2/state/last_file.txt", file, True)

print("Incremental load completed!")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/project2/incremental"))


# COMMAND ----------

df_inc = spark.read.parquet("dbfs:/project2/incremental")
display(df_inc)


# COMMAND ----------

new_batch = """id,name,age,salary,city
16,Arti,29,61000,Pune
17,Rohan,34,72000,Chennai
"""

dbutils.fs.put("dbfs:/project2/raw/batch_2025_04.csv", new_batch, True)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/project2/raw"))


# COMMAND ----------

df_inc = spark.read.parquet("dbfs:/project2/incremental")
display(df_inc)


# COMMAND ----------

print(dbutils.fs.head("dbfs:/project2/state/last_file.txt"))


# COMMAND ----------

dbutils.fs.rm("dbfs:/project2/incremental", True)
dbutils.fs.mkdirs("dbfs:/project2/incremental")


# COMMAND ----------

dbutils.fs.put("dbfs:/project2/state/last_file.txt", "", True)
