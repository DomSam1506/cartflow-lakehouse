data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "cartflow-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue_access" {
  statement {
    sid     = "ReadRawAndScripts"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.zones["raw"].arn}/*",
      "${aws_s3_bucket.zones["scripts"].arn}/*",
    ]
  }

  statement {
    sid     = "ListBuckets"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.zones["raw"].arn,
      aws_s3_bucket.zones["scripts"].arn,
      aws_s3_bucket.zones["clean"].arn,
      aws_s3_bucket.zones["quarantine"].arn,
    ]
  }

  # Dynamic partition overwrite deletes the old files, so DeleteObject is required.
  statement {
    sid     = "WriteCleanAndQuarantine"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.zones["clean"].arn}/*",
      "${aws_s3_bucket.zones["quarantine"].arn}/*",
    ]
  }

  statement {
    sid       = "GlueTempDir"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.zones["scripts"].arn}/tmp/*"]
  }

  statement {
    sid       = "Watermarks"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem"]
    resources = [aws_dynamodb_table.watermarks.arn]
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "cartflow-glue-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_access.json
}