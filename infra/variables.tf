# --------------------------------------------------------------
# VARIABLES
# --------------------------------------------------------------

variable "region" {
    description = "AWS region to deploy (e.g. eu-central-1)."
    type        = string
}

variable "profile" {
    description = "Profile for deploys"
    type        = string
}

variable "bucket_name" {
    description = "Name of the S3 bucket to create"
    type        = string
}

variable "environment" {
    description = "environment name"
    type        = string
}

variable "glue_database_name" {
    description = "Glue database name for raw bronze tables"
    type        = string
}

variable "glue_crawler_name" {
    description = "Glue crawler name for EV sessions raw data"
    type        = string
}

variable "athena_workgroup_name" {
    description = "Athena workgroup name"
    type        = string
}

variable "athena_user_name" {
    description = "Existing IAM user to attach Athena policy to (leave null to skip attachment)"
    type        = string
    default     = null
}