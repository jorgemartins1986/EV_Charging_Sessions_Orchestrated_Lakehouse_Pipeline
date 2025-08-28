# --------------------------------------------------------------
# GLUE DATABASES
# --------------------------------------------------------------

resource "aws_glue_catalog_database" "bronze" {
  name = var.glue_bronze_database_name
}

resource "aws_glue_catalog_database" "silver" {
  name = var.glue_silver_database_name
}
