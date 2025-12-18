# Project 1 – Medallion Architecture Pipeline

## Overview
This project demonstrates an end-to-end Azure Databricks data engineering pipeline using the Medallion Architecture (Bronze, Silver, Gold).

## Architecture
- Source data ingested into Bronze layer
- Data cleaned and transformed in Silver layer
- Business-ready aggregates created in Gold layer

## Technologies Used
- Azure Databricks
- PySpark
- Delta Lake
- Azure Data Lake Storage (ADLS Gen2)

## Key Features
- Structured medallion layers
- Incremental data processing
- Optimized Delta tables
- Scalable Spark transformations

## Folder Structure
- notebooks/ : Databricks notebooks
- data/ : Sample input data
- screenshots/ : Pipeline execution screenshots

## How to Run
1. Upload notebooks to Azure Databricks
2. Configure ADLS access
3. Run Bronze → Silver → Gold notebooks in order

