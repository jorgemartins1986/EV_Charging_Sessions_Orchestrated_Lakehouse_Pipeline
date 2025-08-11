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