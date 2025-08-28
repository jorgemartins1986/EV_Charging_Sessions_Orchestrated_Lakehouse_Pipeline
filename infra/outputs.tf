# --------------------------------------------------------------
# S3
# --------------------------------------------------------------
output "lake_bucket_name" {
  description = "Primary data lake bucket"
  value       = aws_s3_bucket.lake.bucket
}

output "lake_bucket_arn" {
  description = "ARN of the lake bucket"
  value       = aws_s3_bucket.lake.arn
}

output "raw_prefix_uri" {
  description = "S3 URI to the raw EV sessions prefix"
  value       = "s3://${aws_s3_bucket.lake.bucket}/bronze/raw/"
}

output "dataset_object_uri" {
  description = "S3 URI of the uploaded sample CSV"
  value       = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.input_data.key}"
}

output "athena_results_uri" {
  description = "S3 URI where Athena writes query results"
  value       = "s3://${aws_s3_bucket.lake.bucket}/athena/results/"
}

# --------------------------------------------------------------
# Glue
# --------------------------------------------------------------
output "glue_database_name" {
  description = "Glue database for bronze tables"
  value       = aws_glue_catalog_database.bronze.name
}

output "glue_crawler_name" {
  description = "Glue crawler for raw EV sessions"
  value       = aws_glue_crawler.ev_sessions_raw.name
}

# If your provider version exposes it, this will work; otherwise omit.
output "glue_crawler_arn" {
  description = "ARN of the Glue crawler"
  value       = try(aws_glue_crawler.ev_sessions_raw.arn, null)
}

# --------------------------------------------------------------
# Athena
# --------------------------------------------------------------
output "athena_workgroup_name" {
  description = "Athena workgroup to use"
  value       = aws_athena_workgroup.wg.name
}

# --------------------------------------------------------------
# IAM (handy for wiring later)
# --------------------------------------------------------------
output "glue_crawler_role_arn" {
  description = "IAM role ARN assumed by the Glue crawler"
  value       = aws_iam_role.glue_crawler_role.arn
}

output "athena_policy_arn" {
  description = "Reusable policy for Athena + Catalog + S3 access"
  value       = aws_iam_policy.athena_query_policy.arn
}