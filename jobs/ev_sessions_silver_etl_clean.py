import os
import sys, argparse, json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Parse args
DEFAULTS = {
    "dataset": "station",
    "target_bucket": "ev-charging-sessions-1050497-lake",
    "input_prefix": "bronze/raw/",
    "silver_prefix": "silver/ev_sessions_clean/",
    "quarantine_prefix": "bronze/quarantine/ev_sessions_bad/",
    "secondary_partition": "stationId",  # or locationId
    "fail_mode": "quarantine",           # or "fail_job"
}

def parse_args():
    params = [
        "JOB_NAME", "dataset", "target_bucket", "input_prefix", "silver_prefix",
        "quarantine_prefix", "secondary_partition", "fail_mode"
    ]
    args = getResolvedOptions(sys.argv, params)
    return {**DEFAULTS, **args}

cfg = parse_args()


# Spark session
spark = SparkSession.builder.appName(f"ev-sessions-silver-etl-{cfg['dataset']}").getOrCreate()
glue_ctx = GlueContext(spark.sparkContext)
job = Job(glue_ctx)
job.init(cfg["JOB_NAME"], cfg)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.files.ignoreEmptyFiles", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Spark version fix
spark_version = spark.version  # e.g. '3.5.0-amzn-0'
os.environ.setdefault("SPARK_VERSION", ".".join(spark_version.split(".")[:2]))  # -> '3.5'


# Input / Output paths
SRC  = f"s3://{cfg['target_bucket']}/{cfg['input_prefix']}"
OUT  = f"s3://{cfg['target_bucket']}/{cfg['silver_prefix']}"
QOUT = f"s3://{cfg['target_bucket']}/{cfg['quarantine_prefix']}"


# Read bronze (CSV)
df = spark.read.option("header", True).option("inferSchema", True).csv(SRC + "station_data_dataverse.csv")

print("Rows:", df.count())
df.show(5)

# Drop unnecessary columns
df = df.drop("Mon", "Tues", "Wed", "Thurs", "Fri", "Sat", "Sun", "reportedZip", "startTime", "endTime")

# Cast/normalize key columns as needed
df = (df
      .withColumn("sessionId", F.col("sessionId").cast(StringType()))
      .withColumn("userId", F.col("userId").cast(StringType()))
      .withColumn("stationId", F.col("stationId").cast(StringType()))
      .withColumn("locationId", F.col("locationId").cast(StringType()))
      .withColumn("kwhTotal", F.col("kwhTotal").cast(DoubleType()))
      .withColumn("dollars", F.col("dollars").cast(DoubleType()))
      .withColumn("distance", F.col("distance").cast(DoubleType()))
      .withColumn("chargeTimeHrs", F.col("chargeTimeHrs").cast(DoubleType()))
      .withColumn("facilityType", F.col("facilityType").cast(StringType()))
      .withColumn("platform", F.col("platform").cast(StringType()))
      .withColumn("weekday", F.col("weekday").cast(StringType()))
      .withColumn("created", F.col("created").cast(StringType()))
      .withColumn("ended", F.col("ended").cast(StringType()))
)

# Fix 'created' and 'ended' years if they start with '00'
def fix_year(col):
    return F.when(
        F.substring(col, 1, 2) == "00",
        F.concat(F.lit("20"), F.substring(col, 3, 14))
    ).otherwise(col)

df = (df
      .withColumn("created", F.to_timestamp(fix_year(F.col("created"))))
      .withColumn("ended", F.to_timestamp(fix_year(F.col("ended"))))
)

# Create event_date column from created
df = df.withColumn("event_date", F.to_date("created"))

# Map facilityType codes to descriptive names
facility_type_map = {
    "1": "Manufacturing",
    "2": "Office",
    "3": "Research and Development",
    "4": "Other"
}

facility_map_expr = F.create_map([F.lit(x) for x in sum(facility_type_map.items(), ())])

df = df.withColumn("facilityType",
                   F.when(F.col("facilityType").isin(list(facility_type_map.keys())),
                          facility_map_expr[F.col("facilityType")])
                    .otherwise(F.col("facilityType")))

# Map weekday abreviation to full name
weekday_map = {
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
    "Sun": "Sunday"
}

weekday_map_expr = F.create_map([F.lit(x) for x in sum(weekday_map.items(), ())])

df = df.withColumn("weekday",
                   F.when(F.col("weekday").isin(list(weekday_map.keys())),
                          weekday_map_expr[F.col("weekday")])
                    .otherwise(F.col("weekday")))

df.show(5)

# PyDeequ checks
print("Starting PyDeequ checks...")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

check = (Check(spark, CheckLevel.Error, "silver_rules")
    # Completeness
    .isComplete("sessionId")
    .isComplete("userId")
    .isComplete("stationId")
    .isComplete("locationId")
    # Non-negative numerics
    .isNonNegative("kwhTotal")
    .isNonNegative("dollars")
    .isNonNegative("chargeTimeHrs")
    .isNonNegative("distance")
    # Station type domain (1..4)
    .isContainedIn("facilityType", ["Manufacturing","Office","Research and Development","Other"])
    # Uniqueness: sessionId + chargingSessionId (if both exist)
    .isUnique("sessionId")
)

vr = (VerificationSuite(spark)
      .onData(df)
      .addCheck(check)
      .run())

# fail the job when checks fail
if cfg["fail_mode"] == "fail_job" and str(vr.status) != "Success":
    print(f"Data quality FAILED: {vr.status}")
    job.commit()      # ensure cleanup logs; Glue still marks as Failed because of the raised error
    raise RuntimeError("PyDeequ checks failed")

print("All checks passed!")


# Row-level quarantine reasons
rules = [
    F.when(F.col("sessionId").isNull(), F.lit("sessionId_null")).otherwise(F.lit(None)),
    F.when(F.col("userId").isNull(), F.lit("userId_null")).otherwise(F.lit(None)),
    F.when(F.col("stationId").isNull(), F.lit("stationId_null")).otherwise(F.lit(None)),
    F.when(F.col("locationId").isNull(), F.lit("locationId_null")).otherwise(F.lit(None)),
    F.when(F.col("kwhTotal").isNull() | (F.col("kwhTotal") <= 0), F.lit("kwhTotal_non_positive")).otherwise(F.lit(None)),
    F.when(F.col("dollars").isNull() | (F.col("dollars") < 0), F.lit("dollars_negative")).otherwise(F.lit(None)),
    F.when(F.col("distance").isNull() | (F.col("distance") < 0), F.lit("distance_negative_or_zero")).otherwise(F.lit(None)),
    F.when(F.col("chargeTimeHrs").isNull() | (F.col("chargeTimeHrs") <= 0), F.lit("duration_invalid")).otherwise(F.lit(None)),
    F.when(~F.col("facilityType").isin("Manufacturing","Office","Research and Development","Other"),
           F.lit("facilityType_invalid")).otherwise(F.lit(None)),
    F.when(F.col("created").isNull() | F.col("ended").isNull(), F.lit("timestamp_null")).otherwise(F.lit(None)),
    F.when(F.col("created").isNotNull() & F.col("ended").isNotNull() & (F.col("ended") <= F.col("created")),
           F.lit("end_before_start")).otherwise(F.lit(None)),
]

# Build array, drop nulls, and (optionally) convert [] -> null
df = (
    df.withColumn("_reasons", F.array(*rules))
      .withColumn("quarantine_reason", F.expr("filter(_reasons, x -> x is not null)"))
      .drop("_reasons")
)

# # If you prefer NULL instead of [] when no reasons:
# df = df.withColumn(
#     "quarantine_reason",
#     F.when(F.size("quarantine_reason") == 0, F.lit(None)).otherwise(F.col("quarantine_reason"))
# )

# Split & write
secondary = cfg["secondary_partition"]
partition_cols = ["event_date", secondary] if secondary in df.columns else ["event_date"]

good = df.filter(F.size("quarantine_reason") == 0)
good = good.drop("quarantine_reason")
bad  = df.filter(F.size("quarantine_reason") > 0)

good_count = good.count()
bad_count  = bad.count()

good.show(5)
bad.show(5)
print(f"Good rows: {good_count}, Bad rows: {bad_count}")

# Write to parquet
(good.write
 .mode("overwrite")
 .option("compression", "zstd")
 .partitionBy(*partition_cols)
 .parquet(OUT))

(bad.write
 .mode("overwrite")
 .option("compression", "zstd")
 .partitionBy(*partition_cols)
 .parquet(QOUT))

# Gate: fail only if configured to fail on bad rows
if bad_count > 0 and cfg["fail_mode"].lower() == "fail_job":
    raise RuntimeError(f"Quality gate failed: {bad_count} quarantined rows")

print(f"ETL complete. Partitions={partition_cols}. Good={good_count}, Quarantined={bad_count}")


# # Create database and table
# db = "ev_sessions_silver"
# table = "ev_sessions_clean"
# spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")

# # Create an empty external table pointing to the location (let schema be inferred by crawler or define it explicitly)
# spark.sql(f"""
#   CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` (
#     sessionId STRING,
#     userId STRING,
#     stationId STRING,
#     locationId STRING,
#     kwhTotal DOUBLE,
#     dollars DOUBLE,
#     distance DOUBLE,
#     chargeTimeHrs DOUBLE,
#     facilityType STRING,
#     platform STRING,
#     weekday STRING,
#     created TIMESTAMP,
#     ended TIMESTAMP
#   )
#   PARTITIONED BY (event_date DATE, stationId STRING)
#   STORED AS PARQUET
#   LOCATION '{OUT}'
# """)

# # Make partitions visible (needed when you wrote files directly)
# spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table}`")
# print("Registered existing silver data in Glue Catalog.")

# this ends the Glue run cleanly
spark.catalog.clearCache()
job.commit()
spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()