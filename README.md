# Assignment-databricks-pyspark-bookings-analysis-week_03

# Databricks PySpark Bookings Analysis

A hands-on **Databricks and PySpark assignment** covering common Data Engineering operations using booking, hotel, and customer data.

## 📌 Project Overview

This assignment demonstrates how to:

* Read and inspect CSV data
* Select and filter DataFrames
* Create derived columns
* Perform aggregations
* Join multiple DataFrames
* Use Spark SQL
* Apply window functions
* Write data as Parquet and Delta
* Build a complete PySpark transformation pipeline

The project uses three datasets:

* `bookings.csv`
* `hotels.csv`
* `customers.csv`

## 🛠️ Technologies

* Python
* PySpark
* Apache Spark
* Databricks
* Spark SQL
* Delta Lake
* Parquet

## 📚 Assignment Tasks

### 1. Read & Inspect

Inspect the bookings DataFrame using:

```python
printSchema()
show()
count()
describe()
```

### 2. Select & Filter

Find completed bookings with:

* Amount > 10,000
* City = Goa or Mumbai

Uses `F.col()`, `&`, and `.isin()`.

### 3. Derived Columns

Create:

* `amount_with_gst`
* `value_tier`
* `booking_month`

Uses `withColumn()`, `when()`, `otherwise()`, and date functions.

### 4. Aggregations

Calculate city-level:

* Booking count
* Total revenue
* Average amount
* Biggest booking
* Unique customers

### 5. Joins

Practice:

* Inner join
* Left join
* Left anti join
* Three-way join

Also check for orphaned bookings.

### 6. Spark SQL & Window Functions

Use Spark SQL for revenue by hotel category and a window function to find the **top 3 hotels by revenue in each city**.

### 7. Write Results

Save the city-revenue result as:

* Parquet
* Delta table

Then read the Delta table back to verify it.

### 8. Chained Pipeline

Build one PySpark chain to find the **top 5 cities by revenue among hotels rated 4.0 or higher**.

## 🧠 Key Concepts

```text
DataFrames
Filtering
withColumn
Aggregations
Joins
Spark SQL
Window Functions
Parquet
Delta Lake
Data Transformation
```

## 📁 Repository Structure

```text
databricks-pyspark-bookings-analysis/
│
├── README.md
├── notebooks/
│   └── bookings_analysis
└── outputs/
```

## 🎯 Learning Objective

The goal of this assignment is to build practical experience with **PySpark and Databricks workflows commonly used in Data Engineering**.

## 👨‍💻 Author

**Sagar Soni**
Data Engineer | Data Analytics

**Status:** Completed
**Environment:** Databricks
**Language:** Python
**Framework:** PySpark

