# silver_etl_clean.py
# Glue 5.0 (Spark 3.5). Validates with PyDeequ, writes silver + quarantine.

import sys, argparse, json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Try Glue context if present, but keep it runnable locally.
try:
    from awsglue.utils import getResolvedOptions
    GLUE = True
except Exception:
    GLUE = False


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
    if GLUE:
        params = [
            "dataset","target_bucket","input_prefix","silver_prefix",
            "quarantine_prefix","secondary_partition","fail_mode"
        ]
        args = getResolvedOptions(sys.argv, params)
        return {**DEFAULTS, **args}
    else:
        parser = argparse.ArgumentParser()
        for k, v in DEFAULTS.items():
            parser.add_argument(f"--{k}", default=v)
        return vars(parser.parse_args())

cfg = parse_args()


# Spark session
spark = SparkSession.builder.appName(f"silver-etl-{cfg['dataset']}").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# Input / Output paths
SRC  = f"s3://{cfg['target_bucket']}/{cfg['input_prefix']}"
OUT  = f"s3://{cfg['target_bucket']}/{cfg['silver_prefix']}"
QOUT = f"s3://{cfg['target_bucket']}/{cfg['quarantine_prefix']}"


# Read bronze (CSV)
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(SRC))


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
                   F.when(F.col("facilityType").isin(facility_type_map.keys()),
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
                   F.when(F.col("weekday").isin(weekday_map.keys()),
                          weekday_map_expr[F.col("weekday")])
                    .otherwise(F.col("weekday")))


# PyDeequ checks 
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

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

# Optionally persist summary
summary_path = f"s3://{cfg['target_bucket']}/quality_reports/{cfg['dataset']}/event_date={datetime.utcnow().date().isoformat()}/"
try:
    from pydeequ.repository import FileSystemMetricsRepository, ResultKey
    repo = FileSystemMetricsRepository(spark, summary_path)
    key = ResultKey(spark, resultKey=cfg["dataset"])
    repo.save(vr, key)
except Exception:
    pass  # non-fatal if repository not configured


# Row-level quarantine reasons
reasons = [
    F.when(F.col("sessionId").isNull(), F.lit("sessionId_null")),
    F.when(F.col("userId").isNull(), F.lit("userId_null")),
    F.when(F.col("stationId").isNull(), F.lit("stationId_null")),
    F.when(F.col("locationId").isNull(), F.lit("locationId_null")),
    F.when((F.col("kwhTotal").isNull()) | (F.col("kwhTotal") <= 0), F.lit("kwhTotal_non_positive")),
    F.when((F.col("dollars").isNull()) | (F.col("dollars") < 0), F.lit("dollars_negative")),
    F.when((F.col("distance").isNull()) | (F.col("distance") < 0), F.lit("distance_negative")),
    F.when((F.col("chargeTimeHrs").isNull()) | (F.col("chargeTimeHrs") <= 0), F.lit("duration_invalid")),
    F.when(~F.col("facilityType").isin("Manufacturing","Office","Research and Development","Other"), F.lit("facilityType_invalid")),
    # Time order checks
    F.when(F.col("created").isNull() | F.col("ended").isNull(), F.lit("timestamp_null")),
    F.when(F.col("ended") <= F.col("created"), F.lit("end_before_start"))
]

df = df.withColumn("quarantine_reason", F.array_remove(F.array([F.coalesce(x, F.lit(None)) for x in reasons]), None))


# Split & write
secondary = cfg["secondary_partition"]
partition_cols = ["event_date", secondary] if secondary in df.columns else ["event_date"]

good = df.filter(F.size("quarantine_reason") == 0)
good = good.drop("quarantine_reason")
bad  = df.filter(F.size("quarantine_reason") > 0)

good_count = good.count()
bad_count  = bad.count()

# Write to parquet
(good.write
 .mode("append")
 .option("compression", "zstd")
 .partitionBy(*partition_cols)
 .parquet(OUT))

(bad.write
 .mode("append")
 .option("compression", "zstd")
 .partitionBy(*partition_cols)
 .parquet(QOUT))

# Gate: fail only if configured to fail on bad rows
if bad_count > 0 and cfg["fail_mode"].lower() == "fail_job":
    raise RuntimeError(f"Quality gate failed: {bad_count} quarantined rows")

print(f"ETL complete. Partitions={partition_cols}. Good={good_count}, Quarantined={bad_count}")


# Create database and table
db = "ev_sessions_silver"
table = "ev_sessions_clean"
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")

# Create an empty external table pointing to the location (let schema be inferred by crawler or define it explicitly)
spark.sql(f"""
  CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` (
    sessionId STRING,
    userId STRING,
    stationId STRING,
    locationId STRING,
    kwhTotal DOUBLE,
    dollars DOUBLE,
    distance DOUBLE,
    chargeTimeHrs DOUBLE,
    facilityType STRING,
    platform STRING,
    weekday STRING,
    created TIMESTAMP,
    ended TIMESTAMP
  )
  PARTITIONED BY (event_date DATE, stationId STRING)
  STORED AS PARQUET
  LOCATION '{OUT}'
""")

# Make partitions visible (needed when you wrote files directly)
spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table}`")
print("Registered existing silver data in Glue Catalog.")

