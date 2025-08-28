# --------------------------------------------------------------
# ATHENA WORKGROUP (results -> s3://<lake>/athena/results/)
# --------------------------------------------------------------

resource "aws_athena_workgroup" "wg" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena/results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

