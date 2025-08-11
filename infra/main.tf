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

# --------------------------------------------------------------
# GLUE DATABASE
# --------------------------------------------------------------

resource "aws_glue_catalog_database" "bronze" {
    name = var.glue_database_name
}

# --------------------------------------------------------------
# GLUE CRAWLER
# --------------------------------------------------------------

resource "aws_glue_crawler" "ev_sessions_raw" {
    name          = var.glue_crawler_name
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.bronze.name

    s3_target {
        path = "s3://${aws_s3_bucket.lake.bucket}/bronze/raw/"
    }

    configuration = jsonencode({
        Version = 1.0
        Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
    })
}

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

# --------------------------------------------------------------
# IAM ROLE FOR GLUE CRAWLER
# --------------------------------------------------------------

resource "aws_iam_role" "glue_crawler_role" {
    name = "${var.glue_crawler_name}-role"

    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Action    = "sts:AssumeRole"
            Effect    = "Allow"
            Principal = {
                Service = "glue.amazonaws.com"
            }
        }]
    })
}

# --------------------------------------------------------------
# IAM POLICY FOR GLUE CRAWLER
# --------------------------------------------------------------

resource "aws_iam_policy" "glue_crawler_policy" {
    name        = "${var.glue_crawler_name}-policy"
    description = "Policy for Glue crawler to read S3 and update Data Catalog"

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
        # Allow reading from raw prefix
        {
            Effect = "Allow"
            Action = [
                "s3:GetObject",
                "s3:ListBucket"
            ]
            Resource = [
                aws_s3_bucket.lake.arn,
                "${aws_s3_bucket.lake.arn}/bronze/raw/*"
            ]
        },
        # Allow Glue Catalog write access
        {
            Effect = "Allow"
            Action = [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:UpdateTable",
                "glue:DeleteTable"
            ]
            Resource = "*"
        }]
    })
}

# --------------------------------------------------------------
# ATTACH POLICY TO ROLE
# --------------------------------------------------------------

resource "aws_iam_role_policy_attachment" "glue_crawler_attach" {
    role       = aws_iam_role.glue_crawler_role.name
    policy_arn = aws_iam_policy.glue_crawler_policy.arn
}

# --------------------------------------------------------------
# IAM POLICY: Athena query + Glue read + S3 access
# --------------------------------------------------------------

# Identify the lake bucket once (handy for ARNs)
locals {
    lake_bucket_name = aws_s3_bucket.lake.bucket
    lake_bucket_arn  = aws_s3_bucket.lake.arn
}

resource "aws_iam_policy" "athena_query_policy" {
    name        = "athena-query-access"
    description = "Allow running Athena queries, reading Glue Catalog, and S3 access for lake + results"

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
        # Athena query permissions (resource-level control is limited -> use *)
        {
            Effect   = "Allow",
            Action   = [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:ListWorkGroups",
                "athena:GetWorkGroup"
            ],
            Resource = "*"
        },

        # Glue Data Catalog read (scope wide for simplicity)
        {
            Effect   = "Allow",
            Action   = [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:GetCatalogImportStatus"
            ],
            Resource = "*"
        },

        # S3: list buckets + read data
        {
            Effect = "Allow",
            Action = [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            Resource = [
                local.lake_bucket_arn
            ],
            Condition = {
                StringLike = {
                    "s3:prefix" = [
                    "bronze/*",
                    "silver/*",
                    "gold/*",
                    "athena/results/*"
                    ]
                }
            }
        },
        {
            Effect = "Allow",
            Action = [
                "s3:GetObject"
            ],
            Resource = [
                "${local.lake_bucket_arn}/*"
            ]
        },

        # S3: write Athena query outputs to athena/results/
        {
            Effect = "Allow",
            Action = [
                "s3:PutObject"
            ],
            Resource = [
                "${local.lake_bucket_arn}/athena/results/*"
            ]
        }]
    })
}

# --------------------------------------------------------------
# Attach to an existing IAM USER
# --------------------------------------------------------------

resource "aws_iam_user_policy_attachment" "attach_athena_policy_to_user" {
    count      = var.athena_user_name == null ? 0 : 1
    user       = var.athena_user_name
    policy_arn = aws_iam_policy.athena_query_policy.arn
}