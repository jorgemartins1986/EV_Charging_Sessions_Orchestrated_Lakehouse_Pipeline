# --------------------------------------------------------------
# GLUE CRAWLER BRONZE RAW
# --------------------------------------------------------------

resource "aws_glue_crawler" "ev_sessions_raw" {
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.bronze.name
  table_prefix  = "ev_sessions_"

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/bronze/raw/"
  }

  configuration = jsonencode({
    Version  = 1.0
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })
}

# --------------------------------------------------------------
# GLUE CRAWLER SILVER
# --------------------------------------------------------------

resource "aws_glue_crawler" "ev_sessions_silver" {
  name          = var.glue_crawler_silver_name
  role          = aws_iam_role.glue_crawler_silver_role.arn
  database_name = aws_glue_catalog_database.silver.name
  # table_prefix  = "ev_sessions_"

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/silver/ev_sessions_clean/"
  }

  # Only keep keys that belong in configuration
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  # <-- Schema change policy goes here, not in configuration JSON
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG" # or "DELETE_FROM_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  # optional schedule
  # schedule = "cron(0 2 * * ? *)"
}