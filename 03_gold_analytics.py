# Databricks notebook source
from pyspark.sql import functions as F

silver_df = spark.table(
    "workspace.nyc_taxi.silver_trips"
)

daily_kpi_df = (
    silver_df
    .withColumn(
        "trip_date",
        F.to_date("tpep_pickup_datetime")
    )
    .groupBy("trip_date")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare_usd"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles")
    )
    .orderBy("trip_date")
)

display(daily_kpi_df.limit(10))

# COMMAND ----------

(
    daily_kpi_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.nyc_taxi.gold_daily_kpi")
)

gold_count = spark.table(
    "workspace.nyc_taxi.gold_daily_kpi"
).count()

print("Gold Daily KPI table created!")
print(f"Days processed: {gold_count}")

# COMMAND ----------

from pyspark.sql import functions as F

hourly_demand_df = (
    silver_df
    .withColumn(
        "day_of_week",
        F.date_format("tpep_pickup_datetime", "EEEE")
    )
    .withColumn(
        "hour_of_day",
        F.hour("tpep_pickup_datetime")
    )
    .groupBy("day_of_week", "hour_of_day")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare_usd")
    )
    .orderBy(F.desc("total_trips"))
)

display(hourly_demand_df.limit(10))

# COMMAND ----------

(
    hourly_demand_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.nyc_taxi.gold_hourly_demand")
)

hourly_count = spark.table(
    "workspace.nyc_taxi.gold_hourly_demand"
).count()

print("Gold Hourly Demand table created!")
print(f"Day-hour combinations: {hourly_count}")

# COMMAND ----------

from pyspark.sql import functions as F

top_zips_df = (
    silver_df
    .groupBy("pickup_zip")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare_usd"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles")
    )
    .orderBy(F.desc("total_trips"))
)

display(top_zips_df.limit(10))

# COMMAND ----------

(
    top_zips_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.nyc_taxi.gold_top_zips")
)

zip_count = spark.table(
    "workspace.nyc_taxi.gold_top_zips"
).count()

print("Gold Top ZIPs table created!")
print(f"Pickup ZIP codes: {zip_count}")

# COMMAND ----------

silver_count = spark.table(
    "workspace.nyc_taxi.silver_trips"
).count()

gold_daily = spark.table(
    "workspace.nyc_taxi.gold_daily_kpi"
)

gold_hourly = spark.table(
    "workspace.nyc_taxi.gold_hourly_demand"
)

gold_zips = spark.table(
    "workspace.nyc_taxi.gold_top_zips"
)

daily_total = gold_daily.agg(
    {"total_trips": "sum"}
).first()[0]

hourly_total = gold_hourly.agg(
    {"total_trips": "sum"}
).first()[0]

zip_total = gold_zips.agg(
    {"total_trips": "sum"}
).first()[0]

print(f"Silver records: {silver_count:,}")
print(f"Daily KPI total: {daily_total:,}")
print(f"Hourly Demand total: {hourly_total:,}")
print(f"Top ZIPs total: {zip_total:,}")

assert silver_count == daily_total
assert silver_count == hourly_total
assert silver_count == zip_total

print("ALL GOLD DATA QUALITY CHECKS PASSED!")