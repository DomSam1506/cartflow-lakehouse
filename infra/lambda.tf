data "archive_file" "trigger" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/trigger"
  output_path = "${path.module}/../build/trigger.zip"
  excludes    = ["__pycache__"]
}

resource "aws_cloudwatch_log_group" "trigger" {
  name              = "/aws/lambda/cartflow-trigger"
  retention_in_days = 7
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "trigger" {
  name               = "cartflow-trigger"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "trigger_access" {
  statement {
    sid       = "StartTheOneJob"
    actions   = ["glue:StartJobRun"]
    resources = ["arn:aws:glue:${local.region}:${local.account_id}:job/${aws_glue_job.clean_daily.name}"]
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.trigger.arn}:*"]
  }
}

resource "aws_iam_role_policy" "trigger" {
  name   = "cartflow-trigger-access"
  role   = aws_iam_role.trigger.id
  policy = data.aws_iam_policy_document.trigger_access.json
}

resource "aws_lambda_function" "trigger" {
  function_name    = "cartflow-trigger"
  role             = aws_iam_role.trigger.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.trigger.output_path
  source_code_hash = data.archive_file.trigger.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      JOB_NAME = aws_glue_job.clean_daily.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.trigger, aws_iam_role_policy.trigger]
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id   = "AllowS3Invoke"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.trigger.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.zones["raw"].arn
  source_account = local.account_id
}

resource "aws_s3_bucket_notification" "raw_ready" {
  bucket = aws_s3_bucket.zones["raw"].id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_READY"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}