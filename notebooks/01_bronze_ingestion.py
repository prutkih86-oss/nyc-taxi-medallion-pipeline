# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

BASE = "workspace.nyc_taxi"
TARGET = f"{BASE}.bronze_trips"
dbutils.widgets.text(
    "source_table",
    "samples.nyctaxi.trips",
    "Source table"
)

SOURCE = dbutils.widgets.get("source_table")

print(f"Source table: {SOURCE}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BASE}")

# Read incoming source data
incoming = (
    spark.table(SOURCE)
    .withColumn("source_system", F.lit("nyc_taxi_sample"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

trip_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "pickup_zip",
    "dropoff_zip"
]

incoming = incoming.withColumn(
    "trip_key",
    F.sha2(
        F.to_json(F.struct(*[
            F.col(c) for c in trip_columns
        ])),
        256
    )
)

# Initial load or incremental load
if not spark.catalog.tableExists(TARGET):

    (
        incoming.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(TARGET)
    )

    print("INITIAL BRONZE LOAD COMPLETED")

else:

    target = DeltaTable.forName(spark, TARGET)

    (
        target.alias("t")
        .merge(
            incoming.alias("s"),
            "t.trip_key = s.trip_key"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("INCREMENTAL BRONZE MERGE COMPLETED")

result = spark.table(TARGET)

total = result.count()
unique = result.select("trip_key").distinct().count()

print(f"Bronze records: {total:,}")
print(f"Unique keys: {unique:,}")

assert total == unique

print("BRONZE PIPELINE COMPLETED SUCCESSFULLY!")
