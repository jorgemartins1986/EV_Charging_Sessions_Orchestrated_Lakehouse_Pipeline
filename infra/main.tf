# --------------------------------------------------------------
# FOLDER STRUCTURE (placeholders)
# --------------------------------------------------------------

resource "aws_s3_object" "lake_prefixes" {
  for_each     = toset(local.lake_prefixes)
  bucket       = aws_s3_bucket.lake.id
  key          = each.value
  content      = ""
  content_type = "application/x-directory"
}

# --------------------------------------------------------------
# Upload PyDeequ ZIP (if you want to use --extra-py-files)
# --------------------------------------------------------------

resource "aws_s3_object" "pydeequ_zip" {
  bucket       = aws_s3_bucket.lake.id
  key          = "dependencies/pydeequ.zip"
  source       = abspath("${path.module}/../dependencies/pydeequ.zip")
  etag         = filemd5("${path.module}/../dependencies/pydeequ.zip")
  content_type = "application/zip"
}

# --------------------------------------------------------------
# Upload Deequ JAR for Spark (Glue) – pick version that matches Spark
# --------------------------------------------------------------

resource "aws_s3_object" "deequ_jar" {
  bucket       = aws_s3_bucket.lake.id
  key          = "dependencies/deequ-2.0.10-spark-3.5.jar"
  source       = abspath("${path.module}/../dependencies/deequ-2.0.10-spark-3.5.jar")
  etag         = filemd5("${path.module}/../dependencies/deequ-2.0.10-spark-3.5.jar")
  content_type = "application/java-archive"
}

# --------------------------------------------------------------
# ADD THE DATASET (CSV FILE) TO THE S3 BUCKET
# --------------------------------------------------------------

resource "aws_s3_object" "input_data" {
  bucket       = aws_s3_bucket.lake.id
  key          = "bronze/raw/station_data_dataverse.csv"
  source       = abspath("${path.module}/../data-samples/station_data_dataverse.csv")
  etag         = filemd5("${path.module}/../data-samples/station_data_dataverse.csv")
  content_type = "text/csv"
}

# --------------------------------------------------------------
# Upload Silver Glue job script
# --------------------------------------------------------------
resource "aws_s3_object" "glue_script_silver_clean" {
  bucket       = aws_s3_bucket.lake.id
  key          = "jobs/ev_sessions_silver_etl_clean.py" # must match aws_glue_job.command.script_location
  source       = abspath("${path.module}/../jobs/ev_sessions_silver_etl_clean.py")
  etag         = filemd5("${path.module}/../jobs/ev_sessions_silver_etl_clean.py")
  content_type = "text/x-python"
}

# --------------------------------------------------------------
# Upload Gold Glue job script
# --------------------------------------------------------------
resource "aws_s3_object" "glue_script_gold" {
  bucket       = aws_s3_bucket.lake.id
  key          = "jobs/ev_sessions_gold_etl.py" # must match aws_glue_job.command.script_location
  source       = abspath("${path.module}/../jobs/ev_sessions_gold_etl.py")
  etag         = filemd5("${path.module}/../jobs/ev_sessions_gold_etl.py")
  content_type = "text/x-python"
}

# --------------------------------------------------------------
# IAM ROLE FOR BRONZE GLUE CRAWLER
# --------------------------------------------------------------

resource "aws_iam_role" "glue_crawler_role" {
  name = "${var.glue_crawler_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

# --------------------------------------------------------------
# IAM POLICY FOR BRONZE GLUE CRAWLER
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

resource "aws_iam_policy" "athena_query_policy" {
  name        = "athena-query-access"
  description = "Allow running Athena queries, reading Glue Catalog, and S3 access for lake + results"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Athena query permissions (resource-level control is limited -> use *)
      {
        Effect = "Allow",
        Action = [
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
        Effect = "Allow",
        Action = [
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
# IAM ROLE FOR GLUE ETL SILVER JOB
# --------------------------------------------------------------
resource "aws_iam_role" "glue_etl_silver" {
  name = "glue-etl-silver-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

# --------------------------------------------------------------
# ATTACH INLINE POLICY TO GLUE ROLE
# --------------------------------------------------------------
resource "aws_iam_role_policy" "glue_silver_policy" {
  name = "glue-etl-silver-policy"
  role = aws_iam_role.glue_etl_silver.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3LakeAccess"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${aws_s3_bucket.lake.id}"
      },
      {
        Sid      = "S3LakeObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${aws_s3_bucket.lake.id}/*"
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchMetrics",
        Effect = "Allow",
        Action = [
          "cloudwatch:PutMetricData"
        ],
        Resource = "*"
      },
      {
        Sid    = "GlueCatalog",
        Effect = "Allow",
        Action = [
          "glue:CreateDatabase",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetTable",
          "glue:GetTables",

          # Partitions (needed for MSCK REPAIR and partition ops)
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:UpdatePartition",
          "glue:DeletePartition"
        ],
        Resource = "*"
      }
    ]
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

# --------------------------------------------------------------
# IAM ROLE FOR GLUE SILVER CRAWLER
# --------------------------------------------------------------

resource "aws_iam_role" "glue_crawler_silver_role" {
  name = "${var.glue_crawler_silver_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# --------------------------------------------------------------
# IAM POLICY FOR GLUE SILVER CRAWLER
# --------------------------------------------------------------

resource "aws_iam_policy" "glue_crawler_silver_policy" {
  name        = "${var.glue_crawler_silver_name}-policy"
  description = "Policy for Glue crawler to read S3 and update Data Catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # --- S3 list on the bucket (limit to silver prefix) ---
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "silver/ev_sessions_clean/*",
              "silver/ev_sessions_clean"
            ]
          }
        }
      },
      # --- S3 read objects under silver prefix ---
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.lake.arn}/silver/ev_sessions_clean/*"
      },
      # --- Glue Catalog permissions (incl. partitions) ---
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
          "glue:CreateTable", "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition",
          "glue:UpdatePartition", "glue:BatchGetPartition", "glue:BatchUpdatePartition", "glue:DeletePartition"
        ]
        Resource = "*"
      }
      # If your bucket uses a CMK, also add kms:Decrypt for that key
      # {
      #   Effect = "Allow"
      #   Action = ["kms:Decrypt"]
      #   Resource = "<kms-key-arn>"
      # }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_crawler_silver_attach" {
  role       = aws_iam_role.glue_crawler_silver_role.name
  policy_arn = aws_iam_policy.glue_crawler_silver_policy.arn
}

# --------------------------------------------------------------
# IAM ROLE FOR GOLD GLUE ETL JOB
# --------------------------------------------------------------
resource "aws_iam_role" "glue_gold_role" {
  name = "glue-ev-sessions-gold-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# Policy with S3 (silver read + gold warehouse RW) and Glue Catalog permissions
data "aws_iam_policy_document" "glue_gold_policy_doc" {
  # List bucket for relevant prefixes
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "silver/ev_sessions_clean", "silver/ev_sessions_clean/*",
        "gold/warehouse", "gold/warehouse/*"
      ]
    }
  }

  # Read Silver parquet
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/silver/ev_sessions_clean/*"
    ]
  }

  # Full RW on the Iceberg warehouse path (Iceberg writes data + metadata files)
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListBucketMultipartUploads"
    ]
    resources = [
      "${aws_s3_bucket.lake.arn}/gold/warehouse/*"
    ]
  }

  # Glue Catalog permissions (Iceberg uses the Glue Data Catalog as its catalog)
  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:UpdateDatabase",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetTableVersions", "glue:CreateTableVersion"
    ]
    resources = ["*"]
  }

  # List the bucket for the jobs prefix (so Glue can find the script)
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "jobs", "jobs/*", # <-- add jobs prefix
        "silver/ev_sessions_clean", "silver/ev_sessions_clean/*",
        "gold/warehouse", "gold/warehouse/*"
      ]
    }
  }

  # Read the job script under jobs/
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/jobs/*" # <-- allow the script download
    ]
  }

  # (Optional) If the bucket is KMS-encrypted with a CMK, uncomment and set the key ARN:
  # statement {
  #   effect = "Allow"
  #   actions = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
  #   resources = [aws_kms_key.lake.arn]
  # }
}

resource "aws_iam_policy" "glue_gold_policy" {
  name   = "glue-ev-sessions-gold-policy"
  policy = data.aws_iam_policy_document.glue_gold_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "glue_gold_attach" {
  role       = aws_iam_role.glue_gold_role.name
  policy_arn = aws_iam_policy.glue_gold_policy.arn
}