# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # StayNest - Session 6 Assignment (PySpark Deep Dive)
# MAGIC Work through the 8 tasks below in order. Read the Assignment Questions PDF for the
# MAGIC full detail and acceptance criteria. Fill in each `# TODO` cell, run it, and keep the
# MAGIC output visible. Run on Databricks Free Edition (serverless).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 0 - Setup (already done for you)
# MAGIC Upload `bookings.csv`, `hotels.csv`, `customers.csv` to a Volume, then set `BASE`
# MAGIC to that path and run this cell. Counts should be 12000 / 200 / 2000.

# COMMAND ----------

# Point BASE at YOUR Volume path
BASE = "/Volumes/workspace/default/staynest"

print(spark.version)

read_csv = lambda name: (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE}/{name}.csv"))

bookings_df   = read_csv("bookings")
hotels_df     = read_csv("hotels")
customers_df  = read_csv("customers")

print(f"bookings: {bookings_df.count()}, "
      f"hotels: {hotels_df.count()}, "
      f"customers: {customers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Read and inspect
# MAGIC Show the schema, a few sample rows, the row count, and summary stats for the
# MAGIC numeric columns of `bookings_df`.

# COMMAND ----------

# TODO: printSchema, show(5), count, describe(...)

# Schema
bookings_df.printSchema()

# Sample rows
bookings_df.show(5)

# Row count
print("Row count:", bookings_df.count())

# Summary statistics for numeric column(s)
bookings_df.describe("amount").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Select and filter
# MAGIC From `bookings_df`, select a few useful columns and return the **completed**
# MAGIC bookings with `amount` over 10000 in the cities Goa or Mumbai. Use `col()`, combine
# MAGIC conditions with `&`, and use `.isin(...)`.

# COMMAND ----------

# TODO

from pyspark.sql import functions as F

# Select required columns
completed_booking = bookings_df.select(
    F.col("booking_id"),
    F.col("customer_id"),
    F.col("city"),
    F.col("status"),
    F.col("amount")
).filter(
    # Filter completed bookings with amount > 10000 in Goa or Mumbai
    (F.col("status") == "completed") &
    (F.col("amount") > 10000) &
    (F.col("city").isin("Goa", "Mumbai"))
)

# Display the result
completed_booking.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Derived columns
# MAGIC Add: `amount_with_gst` (amount plus 12% tax), a `value_tier`
# MAGIC (premium / standard / budget) using `when`/`otherwise`, and a `booking_month`
# MAGIC from `booking_date`.

# COMMAND ----------

# TODO

# Add derived columns to the bookings DataFrame
bookings_derived = (
    bookings_df

    # Add 12% GST to the booking amount
    .withColumn(
        "amount_with_gst",
        F.col("amount") * 1.12
    )

    # Categorize bookings based on amount
    .withColumn(
        "value_tier",
        F.when(F.col("amount") >= 50000, "premium")
         .when(F.col("amount") >= 20000, "standard")
         .otherwise("budget")
    )

    # Extract month from booking date
    .withColumn(
        "booking_month",
        F.month(F.col("booking_date"))
    )
)

# Display a sample of the result
display(bookings_derived)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Aggregations
# MAGIC For **completed** bookings, group by `city` and return: number of bookings, total
# MAGIC revenue, average amount, biggest booking, and the count of unique customers.
# MAGIC Order by revenue, highest first.

# COMMAND ----------

# TODO

from pyspark.sql import functions as F

# Filter completed bookings and aggregate by city
city_summary = (
    bookings_df
    .filter(F.col("status") == "completed")
    .groupBy("city")
    .agg(
        F.count("*").alias("booking_count"),
        F.round(F.sum("amount"),2).alias("total_revenue"),
        F.round(F.avg("amount"),2).alias("average_amount"),
        F.round(F.max("amount"),2).alias("biggest_booking"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.col("total_revenue").desc())
)

# Display results
display(city_summary)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Joins
# MAGIC Inner-join bookings to hotels to enrich each booking. Do a left join too. Use
# MAGIC `left_anti` to check for orphaned bookings (expect 0). Then do a three-way join
# MAGIC with customers.

# COMMAND ----------

# TODO

from pyspark.sql import functions as F

# 1. Inner join bookings with hotels
bookings_hotels_inner = (
    bookings_df
    .join(
        hotels_df,
        on="hotel_id",
        how="inner"
    )
)

bookings_hotels_inner.display()


# 2. Left join to keep all bookings
bookings_hotels_left = (
    bookings_df
    .join(
        hotels_df,
        on="hotel_id",
        how="left"
    )
)

bookings_hotels_left.display()


# 3. Find bookings without a matching hotel
orphaned_bookings = (
    bookings_df
    .join(
        hotels_df,
        on="hotel_id",
        how="left_anti"
    )
)

print("Orphaned bookings:", orphaned_bookings.count())


# 4. Three-way join: bookings + hotels + customers
bookings_full = (
    bookings_df
    .join(
        hotels_df,
        on="hotel_id",
        how="inner"
    )
    .join(
        customers_df,
        on="customer_id",
        how="inner"
    )
)

bookings_full.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Spark SQL + a window function
# MAGIC Register temp views and use `spark.sql` to get revenue by hotel `category` for
# MAGIC completed bookings. Then use a window function to rank the **top 3 hotels by
# MAGIC revenue within each city**.

# COMMAND ----------

# TODO: spark.sql(...) for revenue by category

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Register DataFrames as temporary SQL views
bookings_df.createOrReplaceTempView("bookings")
hotels_df.createOrReplaceTempView("hotels")

# Revenue by hotel category for completed bookings
category_revenue = spark.sql("""
    SELECT
        h.category,
        round(SUM(b.amount),2) AS total_revenue
    FROM bookings b
    INNER JOIN hotels h
        ON b.hotel_id = h.hotel_id
    WHERE b.status = 'completed'
    GROUP BY h.category
    ORDER BY total_revenue DESC
""")

# Display SQL result
display(category_revenue)


# COMMAND ----------

# TODO: window function for top 3 hotels per city

# Calculate revenue for each hotel
hotel_revenue = (
    bookings_df
    .filter(F.col("status") == "completed")
    .groupBy("hotel_id")
    .agg(
        F.round(F.sum("amount"),2).alias("total_revenue")
    )
)

# Add hotel details
hotel_revenue = hotel_revenue.join(
    hotels_df.select(
        "hotel_id",
        "hotel_name",
        "city"
    ),
    on="hotel_id",
    how="inner"
)

# Rank hotels within each city by revenue
city_window = (
    Window
    .partitionBy("city")
    .orderBy(F.col("total_revenue").desc())
)

top_3_hotels = (
    hotel_revenue
    .withColumn(
        "revenue_rank",
        F.row_number().over(city_window)
    )
    .filter(F.col("revenue_rank") <= 3)
)

# Display top 3 hotels per city
display(top_3_hotels)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Write the result
# MAGIC Write your city-revenue result as **Parquet**, and also as a **Delta table** with
# MAGIC `saveAsTable`. Read the Delta table back to confirm.

# COMMAND ----------

# TODO

# Save city revenue result as Parquet
city_summary.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/practice/city_revenue")

# Overwrite the existing Delta table and its schema
city_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.city_revenue")

# Read the Delta table back
city_revenue_delta = spark.table("workspace.default.city_revenue")

display(city_revenue_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - One chained pipeline
# MAGIC In a single chain: keep completed bookings, join hotels, keep hotels with
# MAGIC `star_rating >= 4.0`, group by `city`, sum revenue, order descending, take the
# MAGIC top 5. End with one `.show()`.

# COMMAND ----------

# TODO

# Build one pipeline for top 5 cities by revenue
(
    bookings_df
    .filter(F.col("status") == "completed")
    .join(
        hotels_df,
        on="hotel_id",
        how="inner"
    )
    .drop(hotels_df["city"])  # Drop duplicate city from hotel side
    .filter(F.col("star_rating") >= 4.0)
    .groupBy("city")
    .agg(
        F.round(F.sum("amount"),2).alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
    .limit(5)
    .show()
)