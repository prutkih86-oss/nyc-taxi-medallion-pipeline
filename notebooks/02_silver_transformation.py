
from pyspark.sql import functions as F
from delta.tables import DeltaTable

BASE = "workspace.nyc_taxi"

BRONZE = f"{BASE}.bronze_trips"
SILVER = f"{BASE}.silver_trips"
QUARANTINE = f"{BASE}.silver_quarantine"

# 1. Read Bronze
bronze = spark.table(BRONZE)

# 2. Calculate duration and speed
validated = (
    bronze
    .withColumn(
        "duration_minutes",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )
    .withColumn(
        "avg_speed_mph",
        F.when(
            F.col("duration_minutes") > 0,
            F.col("trip_distance") * 60
            / F.col("duration_minutes")
        )
    )
)

# 3. Apply quality rules
validated = validated.withColumn(
    "rejection_reason",
    F.concat_ws(
        "; ",
        F.when(
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") <= 0),
            "INVALID_DISTANCE"
        ),
        F.when(
            F.col("fare_amount").isNull()
            | (F.col("fare_amount") <= 0),
            "INVALID_FARE"
        ),
        F.when(
            F.col("duration_minutes").isNull()
            | (F.col("duration_minutes") <= 0),
            "INVALID_DURATION"
        ),
        F.when(
            F.col("pickup_zip").isNull()
            | F.col("dropoff_zip").isNull(),
            "MISSING_ZIP"
        ),
        F.when(
            F.col("avg_speed_mph") > 80,
            "EXCESSIVE_SPEED"
        )
    )
)

# 4. Split records
silver_batch = validated.filter(
    F.col("rejection_reason") == ""
)

quarantine_batch = validated.filter(
    F.col("rejection_reason") != ""
)

# 5. Validate input before writing
bronze_count = bronze.count()
silver_batch_count = silver_batch.count()
quarantine_batch_count = quarantine_batch.count()

assert bronze_count == silver_batch_count + quarantine_batch_count

assert (
    validated.select("trip_key").distinct().count()
    == bronze_count
)

assert validated.filter(F.col("trip_key").isNull()).count() == 0

print("INPUT DATA QUALITY PASSED")

# 6. Initialize target Delta tables on first run
if not spark.catalog.tableExists(SILVER):
    (
        silver_batch.limit(0)
        .write
        .format("delta")
        .saveAsTable(SILVER)
    )
    print("SILVER TABLE INITIALIZED")

if not spark.catalog.tableExists(QUARANTINE):
    (
        quarantine_batch.limit(0)
        .write
        .format("delta")
        .saveAsTable(QUARANTINE)
    )
    print("QUARANTINE TABLE INITIALIZED")

# 7. Incremental MERGE into Silver
silver_target = DeltaTable.forName(spark, SILVER)

(
    silver_target.alias("t")
    .merge(
        silver_batch.alias("s"),
        "t.trip_key = s.trip_key"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("SILVER MERGE COMPLETED")

# 8. Incremental MERGE into Quarantine
quarantine_target = DeltaTable.forName(
    spark, QUARANTINE
)

(
    quarantine_target.alias("t")
    .merge(
        quarantine_batch.alias("s"),
        "t.trip_key = s.trip_key"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("QUARANTINE MERGE COMPLETED")

# 9. Verify persisted results
silver_result = spark.table(SILVER)
quarantine_result = spark.table(QUARANTINE)

silver_count = silver_result.count()
quarantine_count = quarantine_result.count()

silver_unique = silver_result.select(
    "trip_key"
).distinct().count()

quarantine_unique = quarantine_result.select(
    "trip_key"
).distinct().count()

overlap = (
    silver_result.select("trip_key")
    .join(
        quarantine_result.select("trip_key"),
        "trip_key",
        "inner"
    )
    .count()
)

assert silver_count == silver_unique
assert quarantine_count == quarantine_unique
assert overlap == 0
assert bronze_count == silver_count + quarantine_count

print(f"Bronze: {bronze_count:,}")
print(f"Silver: {silver_count:,}")
print(f"Quarantine: {quarantine_count:,}")

print("INCREMENTAL SILVER PIPELINE SUCCESSFUL!")
