# --------------------------------------------------------------
# GLUE JOB: silver-etl-clean
# --------------------------------------------------------------
resource "aws_glue_job" "silver_etl_clean" {
  name       = "ev-sessions-silver-etl-clean"
  role_arn   = aws_iam_role.glue_etl_silver.arn
  depends_on = [aws_s3_object.glue_script_silver_clean]

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/jobs/ev_sessions_silver_etl_clean.py"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30 # minutes
  max_retries       = 1

  default_arguments = {
    "--dataset"             = "station"
    "--target_bucket"       = aws_s3_bucket.lake.bucket
    "--input_prefix"        = "bronze/raw/"
    "--silver_prefix"       = "silver/ev_sessions_clean/"
    "--quarantine_prefix"   = "bronze/quarantine/ev_sessions_bad/"
    "--secondary_partition" = "stationId"
    "--fail_mode"           = "quarantine"

    # Spark version fix
    "--SPARK_VERSION" = "3.5"

    # Dependencies
    "--extra-py-files" = "s3://${aws_s3_bucket.lake.bucket}/dependencies/pydeequ.zip"
    "--extra-jars"     = "s3://${aws_s3_bucket.lake.bucket}/dependencies/deequ-2.0.10-spark-3.5.jar"

    # Nice-to-haves
    "--enable-metrics" = "true"
    "--job-language"   = "python"
    "--TempDir"        = "s3://${aws_s3_bucket.lake.bucket}/tmp/glue/"
  }

  # Concurrency guard
  execution_property {
    max_concurrent_runs = 1
  }

  # Tags help when showcasing
  tags = {
    Project = "EV-Sessions"
    Layer   = "Silver"
    IaC     = "Terraform"
  }
}

# --------------------------------------------------------------
# GLUE JOB: ev-sessions-gold-etl
# --------------------------------------------------------------
resource "aws_glue_job" "ev_sessions_gold_etl" {
  name              = "ev-sessions-gold-etl"
  role_arn          = aws_iam_role.glue_gold_role.arn
  glue_version      = "5.0" # Spark 3.5
  number_of_workers = 2
  worker_type       = "G.1X" # adjust if needed

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/jobs/ev_sessions_gold_etl.py"
  }

  # Default args passed to your script (can override per-run)
  default_arguments = {
    "--JOB_NAME"            = "ev-sessions-gold-etl"
    "--job-language"        = "python"
    "--enable-metrics"      = "true"
    "--target_bucket"       = aws_s3_bucket.lake.bucket
    "--silver_prefix"       = "silver/ev_sessions_clean/"
    "--gold_warehouse"      = "gold/warehouse/"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--TempDir"             = "s3://${aws_s3_bucket.lake.bucket}/tmp/glue/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  timeout = 30 # minutes;
}