**Brazilian E-Commerce Data Engineering Project**

This project involves building an end-to-end big data pipeline using PySpark for the Brazilian e-commerce dataset (Olist).
Each module represents a key stage in the data engineering workflow — from ingestion to serving.

📁 Project Structure

***Module 1*** - Data Ingestion and Exploration

***Module 2*** - Data Cleaning and Transformation

***Module 3*** - Data Integration and Aggregation

***Module 4*** - Property Optimization

***Module 5*** - Data Serving

***Module Details in depth***

***Module 1 - Data Ingestion and Exploration***

Downloaded the Brazilian E-commerce dataset using Kaggle API.

Created a SparkSession and connected to HDFS.

Loaded raw CSV files into PySpark DataFrames.

Performed initial exploration (schema view, sample data display).

***Module 2 - Data Cleaning and Transformation***

Identified and handled missing values.

Applied necessary data type transformations.

Standardized column formats across datasets (customers, orders, items, payments, reviews, products).

Displayed clean sample records for verification.

***Module 3 - Data Integration and Aggregation***

Integrated multiple datasets using Spark SQL joins (e.g., orders + order items).

Cached frequent tables to improve query performance.

***Aggregated key metrics like below*** :

Order status distribution

Payment type distribution

Top-selling products

Average delivery time calculations

***Module 4 - Property Optimization***

Tuned Spark configurations for better memory management and execution speed:

Executor memory and cores

Parallelism and shuffle partitions

Enabled Adaptive Query Execution (AQE) to optimize queries dynamically.

Revalidated data integrity after optimization.

***Module 5 - Data Serving***

Saved the processed and cleaned data as Parquet files in:

Local HDFS (/olist/proc/)

Google Cloud Storage (GCS Bucket)

Ensured optimized partitioning for faster read/write during future analytics.

***Technologies Used***

PySpark ,HDFS (Hadoop Distributed File System),Google Cloud Storage (for cloud data serving),Python 3.8+

***Dataset***

Brazilian E-commerce Public Dataset by Olist (Kaggle)
