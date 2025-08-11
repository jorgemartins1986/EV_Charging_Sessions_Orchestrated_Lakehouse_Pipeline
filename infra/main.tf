# --------------------------------------------------------------
# CREATE S3 BUCKET
# --------------------------------------------------------------

resource "aws_s3_bucket" "lake" {
    bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "lake" {
    bucket = aws_s3_bucket.lake.id
    versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
    bucket = aws_s3_bucket.lake.id
    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

resource "aws_s3_bucket_public_access_block" "lake" {
    bucket                  = aws_s3_bucket.lake.id
    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true
}

# --------------------------------------------------------------
# FOLDER STRUCTURE (placeholders)
# --------------------------------------------------------------

locals {
    lake_prefixes = [
        "bronze/raw/ev_sessions/",
        "bronze/quarantine/ev_sessions_bad/",
        "silver/ev_sessions_clean/",
        "gold/ev_sessions/",
        "athena/results/"
    ]
}

resource "aws_s3_object" "lake_prefixes" {
    for_each     = toset(local.lake_prefixes)
    bucket       = aws_s3_bucket.lake.id
    key          = each.value
    content      = ""
    content_type = "application/x-directory"
}

# --------------------------------------------------------------
# ADD THE DATASET (CSV FILE) TO THE S3 BUCKET
# --------------------------------------------------------------

resource "aws_s3_bucket_object" "input_data" {
    bucket       = aws_s3_bucket.lake.id
    key          = "bronze/raw/station_data_dataverse.csv"
    source       = abspath("${path.module}/../data-samples/station_data_dataverse.csv")
    etag         = filemd5("${path.module}/../data-samples/station_data_dataverse.csv")
    content_type = "text/csv"
}
