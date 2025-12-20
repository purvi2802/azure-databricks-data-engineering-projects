# Databricks notebook source
print("Starting Orchestration Pipeline...")

# Run ETL notebook
print("Running 01_ETL ...")
dbutils.notebook.run("01_ETL", 0)

# Run Data Quality notebook
print("Running 02_DQ ...")
dbutils.notebook.run("02_DQ", 0)

# Run Incremental Load notebook
print("Running 03_Incremental ...")
dbutils.notebook.run("03_Incremental", 0)

print("Pipeline completed successfully!")

