# --------------------------------------------------------------
# LOCALS
# --------------------------------------------------------------

locals {
  lake_prefixes = [
    "bronze/raw/",
    "bronze/quarantine/ev_sessions_bad/",
    "silver/ev_sessions_clean/",
    "gold/warehouse/",
    "athena/results/",
    "jobs/",
    "tmp/glue/"
  ]
  lake_bucket_name = aws_s3_bucket.lake.bucket
  lake_bucket_arn  = aws_s3_bucket.lake.arn
}
