# ev_sessions_gold_etl.py
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# -----------------------------
# Args / Config
# -----------------------------
DEFAULTS = {
    "target_bucket": "ev-charging-sessions-1050497-lake",
    "silver_prefix": "silver/ev_sessions_clean/",
    # Iceberg warehouse root (where Iceberg data/metadata is stored)
    "gold_warehouse": "gold/warehouse/",
    # Optional: limit processing to a single date (YYYY-MM-DD). Empty = all data.
    "event_date": ""
}

def parse_args():
    # only required args here
    required = ["JOB_NAME", "target_bucket", "silver_prefix", "gold_warehouse"]
    args = getResolvedOptions(sys.argv, required)

    # try to read optional --event_date if provided
    try:
        opt = getResolvedOptions(sys.argv, ["event_date"])
        args.update(opt)
    except Exception:
        # not provided -> use default
        args["event_date"] = DEFAULTS["event_date"]

    # merge over defaults
    return {**DEFAULTS, **args}

cfg = parse_args()

# S3 paths
SILVER_PATH = f"s3://{cfg['target_bucket']}/{cfg['silver_prefix']}"
WAREHOUSE  = f"s3://{cfg['target_bucket']}/{cfg['gold_warehouse']}"
DB_NAME    = "ev_sessions_gold"  # Iceberg namespace
FACT_TABLE = f"glue_catalog.{DB_NAME}.fact_ev_session"

# -----------------------------
# Spark / Glue setup
# -----------------------------
spark = (
    SparkSession.builder
    .appName("ev-sessions-gold-etl")
    .getOrCreate()
)

# Glue context & job (for clean end of run)
glue_ctx = GlueContext(spark.sparkContext)
job = Job(glue_ctx)
job.init(cfg["JOB_NAME"], cfg)

# General Spark tweaks
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Iceberg configs (Glue 5 / Spark 3.5)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
spark.conf.set("spark.sql.catalog.glue_catalog.default-namespace", DB_NAME)
# Optional: Iceberg V2 features
# spark.sql("ALTER CATALOG glue_catalog SET('warehouse'='{}')".format(WAREHOUSE))

print(f"[GOLD] Warehouse: {WAREHOUSE}")
print(f"[GOLD] Reading silver from: {SILVER_PATH}")

# -----------------------------
# Read Silver (good-only path)
# -----------------------------
df = spark.read.parquet(SILVER_PATH).select(
    "sessionId","userId","stationId","locationId",
    "kwhTotal","dollars","distance","chargeTimeHrs",
    "facilityType","platform","weekday","created","ended","event_date"
)

# Optional event_date filter
if cfg["event_date"]:
    print(f"[GOLD] Filtering to event_date={cfg['event_date']}")
    df = df.filter(F.col("event_date") == F.to_date(F.lit(cfg["event_date"])))

row_count = df.count()
print(f"[GOLD] Silver rows to process: {row_count}")

if row_count == 0:
    print("[GOLD] No rows to process. Exiting.")
    job.commit()
    spark.stop()
    sys.exit(0)

# -----------------------------
# Gold Features (derived fields)
# -----------------------------
df_gold = (
    df
    # minutes between timestamps (robust to nulls)
    .withColumn(
        "session_duration_minutes",
        (F.unix_timestamp("ended") - F.unix_timestamp("created")) / F.lit(60.0)
    )
    # average cost per kWh (avoid div-by-zero)
    .withColumn(
        "avg_cost_per_kwh",
        F.when(F.col("kwhTotal") > 0, F.col("dollars") / F.col("kwhTotal"))
    )
)

df_gold.show(5, truncate=False)

# -----------------------------
# Ensure namespace / Create table
# -----------------------------
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{DB_NAME}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
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
  ended TIMESTAMP,
  event_date DATE,
  -- Derived fields
  session_duration_minutes DOUBLE,
  avg_cost_per_kwh DOUBLE
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2')
""")

# -----------------------------
# Overwrite partitions (idempotent)
# -----------------------------
print(f"[GOLD] Writing to Iceberg table: {FACT_TABLE} (overwritePartitions)")
df_gold.writeTo(FACT_TABLE).overwritePartitions()

# Small verification (optional)
res = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FACT_TABLE}")
res.show()

print("[GOLD] Gold load complete.")

# -----------------------------
# Clean shutdown
# -----------------------------
spark.catalog.clearCache()
job.commit()
spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()