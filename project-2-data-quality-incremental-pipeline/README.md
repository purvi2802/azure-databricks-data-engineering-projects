# Project 2 – Data Quality & Incremental Pipeline (Azure Databricks)

## 📌 Overview
This project demonstrates a *production-style data engineering pipeline* built using *Azure Databricks, **PySpark, and **Delta Lake*.

The focus of this project is on:
•⁠  ⁠Incremental data ingestion
•⁠  ⁠Data quality validation
•⁠  ⁠Handling bad records
•⁠  ⁠Delta Lake upserts (MERGE)
•⁠  ⁠Pipeline orchestration

This simulates how real-world data platforms process continuously arriving data safely and efficiently.

---

## 🏗 Architecture Overview

Azure Blob Storage (Raw Data)
↓
Bronze Layer (Incremental Load)
↓
Silver Layer (Data Quality Validation)
↓
Silver Layer (Incremental MERGE / Upserts)
↓
Gold Layer (Metrics & Monitoring)
↓
Analytics / Reporting


All transformations are executed in *Azure Databricks using PySpark and Delta Lake*.

---

## 🥉 Bronze Layer – Incremental Ingestion

*Purpose:*  
Ingest only *new or updated records*, avoiding full reprocessing.

*Key Features:*
•⁠  ⁠Uses a watermark column (e.g. updated_at / ingestion_time)
•⁠  ⁠Filters only newly arrived data
•⁠  ⁠Appends data incrementally to Delta tables

*Why this matters:*  
Incremental ingestion improves performance, scalability, and cost efficiency in production pipelines.

---

## 🥈 Silver Layer – Data Quality Validation

*Purpose:*  
Ensure that only *clean and reliable data* flows downstream.

*Data Quality Rules Implemented:*
•⁠  ⁠Not-null checks on critical columns
•⁠  ⁠Range validation (e.g. quantity > 0)
•⁠  ⁠Basic format checks
•⁠  ⁠Referential integrity checks

*Bad Data Handling:*
•⁠  ⁠Valid records → stored in Silver tables
•⁠  ⁠Invalid records → quarantined separately for analysis

This prevents bad data from corrupting downstream analytics.

---

## 🔄 Silver Incremental Merge (Upserts)

*Purpose:*  
Handle late-arriving data and updates using *Delta Lake MERGE operations*.

*What this achieves:*
•⁠  ⁠Inserts new records
•⁠  ⁠Updates existing records
•⁠  ⁠Ensures idempotent pipeline runs

This mirrors real production systems where source data can change over time.

---

## 🥇 Gold Layer – Metrics & Monitoring

*Purpose:*  
Generate metrics to monitor pipeline health and data quality.

*Metrics Generated:*
•⁠  ⁠Total records processed
•⁠  ⁠Failed record count
•⁠  ⁠Failure percentage
•⁠  ⁠Daily ingestion summary

These metrics help data engineers monitor pipeline reliability and data trust.

---

## ⏱ Pipeline Orchestration

The pipeline is orchestrated using *Databricks Jobs / notebook chaining*.

*Execution Order:*
1.⁠ ⁠Bronze Incremental Load
2.⁠ ⁠Silver Data Quality Validation
3.⁠ ⁠Silver Incremental Merge
4.⁠ ⁠Gold Metrics Generation

This ensures correct dependency management and fault isolation.

---

## 📂 Project Structure

project-2-data-quality-incremental-pipeline/
│
├── notebooks/
│ ├── 01_bronze_incremental.py
│ ├── 02_silver_data_quality.py
│ ├── 03_silver_incremental_merge.py
│ └── 04_pipeline_orchestration.py
│
├── dq_rules/
│ └── dq_rules.yaml
│
├── screenshots/
│ ├── incremental_load.jpeg
│ ├── dq_failures.jpeg
│ └── orchestration.jpeg
│
└── README.md


---

## 🛠 Tech Stack
•⁠  ⁠Azure Databricks
•⁠  ⁠Apache Spark (PySpark)
•⁠  ⁠Delta Lake
•⁠  ⁠Azure Blob Storage
•⁠  ⁠GitHub

---

## 💬 Interview Talking Points
•⁠  ⁠Why incremental ingestion is critical for scalable pipelines
•⁠  ⁠How data quality checks prevent downstream failures
•⁠  ⁠Difference between append and merge in Delta Lake
•⁠  ⁠Handling late-arriving and updated records
•⁠  ⁠Monitoring pipelines using quality metrics

---

## ✅ Project Status
✔ Incremental Bronze Ingestion  
✔ Data Quality Validation  
✔ Delta Lake Upserts  
✔ Pipeline Orchestration  
✔ Monitoring Metrics  

This project represents a *real-world, production-style data engineering pipeline*.
