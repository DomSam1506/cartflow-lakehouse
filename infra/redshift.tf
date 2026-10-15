# infra/redshift.tf
# Everything here is behind var.enable_redshift. Default false = nothing exists = nothing is billed.
#   terraform -chdir=infra apply -var enable_redshift=true     # build it
#   terraform -chdir=infra apply                               # tear it down (default is false)

variable "enable_redshift" {
  description = "Create the Redshift Serverless warehouse (costs money while queries run)"
  type        = bool
  default     = false
}

variable "gold_bucket_name" {
  description = "Bucket that holds the gold Parquet"
  type        = string
  default     = "cartflow-28cddc-gold"
}

# Redshift Serverless needs subnets in at least 3 AZs. The default VPC has one per AZ.
data "aws_vpc" "default" {
  count   = var.enable_redshift ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = var.enable_redshift ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

resource "aws_iam_role" "redshift" {
  count = var.enable_redshift ? 1 : 0
  name  = "cartflow-redshift"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_read_gold" {
  count = var.enable_redshift ? 1 : 0
  name  = "read-gold"
  role  = aws_iam_role.redshift[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
      Resource = [
        "arn:aws:s3:::${var.gold_bucket_name}",
        "arn:aws:s3:::${var.gold_bucket_name}/*",
      ]
    }]
  })
}

resource "aws_redshiftserverless_namespace" "cartflow" {
  count                 = var.enable_redshift ? 1 : 0
  namespace_name        = "cartflow"
  db_name               = "cartflow"
  manage_admin_password = true
  iam_roles             = [aws_iam_role.redshift[0].arn]
  default_iam_role_arn  = aws_iam_role.redshift[0].arn
}

resource "aws_redshiftserverless_workgroup" "cartflow" {
  count               = var.enable_redshift ? 1 : 0
  namespace_name      = aws_redshiftserverless_namespace.cartflow[0].namespace_name
  workgroup_name      = "cartflow"
  base_capacity       = 4
  publicly_accessible = false
  subnet_ids          = data.aws_subnets.default[0].ids
}

# Hard spending guard: after 40 RPU-hours in a month, compute is switched off.
# 40 RPU-hours x about $0.375 = about $15 worst case (check the Ohio price on the Redshift pricing page).
resource "aws_redshiftserverless_usage_limit" "compute" {
  count         = var.enable_redshift ? 1 : 0
  resource_arn  = aws_redshiftserverless_workgroup.cartflow[0].arn
  usage_type    = "serverless-compute"
  amount        = 40
  period        = "monthly"
  breach_action = "deactivate"
}