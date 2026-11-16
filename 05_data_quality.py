# Databricks notebook source
from pyspark.sql import functions as F

BASE = "workspace.nyc_taxi"

bronze = spark.table(f"{BASE}.bronze_trips")
silver = spark.table(f"{BASE}.silver_trips")
quarantine = spark.table(f"{BASE}.silver_quarantine")

daily = spark.table(f"{BASE}.gold_daily_kpi")
hourly = spark.table(f"{BASE}.gold_hourly_demand")
zips = spark.table(f"{BASE}.gold_top_zips")

bronze_count = bronze.count()
silver_count = silver.count()
quarantine_count = quarantine.count()

daily_total = daily.agg(F.sum("total_trips")).first()[0]
hourly_total = hourly.agg(F.sum("total_trips")).first()[0]
zip_total = zips.agg(F.sum("total_trips")).first()[0]

checks = {
    "Bronze reconciliation":
        bronze_count == silver_count + quarantine_count,

    "Daily KPI reconciliation":
        silver_count == daily_total,

    "Hourly demand reconciliation":
        silver_count == hourly_total,

    "ZIP reconciliation":
        silver_count == zip_total,

    "No empty Silver":
        silver_count > 0,

    "Quarantine reasons present":
        quarantine.filter(
            F.col("rejection_reason").isNull()
            | (F.col("rejection_reason") == "")
        ).count() == 0
}

for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")

failed = [name for name, passed in checks.items() if not passed]

if failed:
    raise RuntimeError(f"DATA QUALITY FAILED: {failed}")

print("ALL PIPELINE QUALITY CHECKS PASSED!")