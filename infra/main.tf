
terraform {
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    random  = { source = "hashicorp/random" }
    archive = { source = "hashicorp/archive" }
  }
}

provider "aws" {
  region = "us-east-2"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "random_id" "suffix" { byte_length = 3 }

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
  job_name   = "cartflow-clean-daily"
  p          = "cartflow-${random_id.suffix.hex}"
}


locals {}

resource "aws_s3_bucket" "zones" {
  for_each      = toset(["raw", "clean", "gold", "quarantine", "scripts", "athena-results"])
  bucket        = "${local.p}-${each.key}"
  force_destroy = true # lets `terraform destroy` empty buckets
}

resource "aws_dynamodb_table" "watermarks" {
  name         = "cartflow-watermarks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "table_name"

  attribute {
    name = "table_name"
    type = "S"
  }
}

resource "aws_glue_catalog_database" "cartflow" { name = "cartflow" }

resource "aws_athena_workgroup" "wg" {
  name = "cartflow"
  configuration {
    bytes_scanned_cutoff_per_query = 10737418240 # hard stop at 10 GB scanned
    result_configuration { output_location = "s3://${aws_s3_bucket.zones["athena-results"].bucket}/" }
  }
}