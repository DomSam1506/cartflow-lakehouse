resource "aws_s3_object" "entry" {
  bucket = aws_s3_bucket.zones["scripts"].id
  key    = "clean_daily_entry.py"
  source = "${path.module}/../glue_jobs/glue_entry.py"
  etag   = filemd5("${path.module}/../glue_jobs/glue_entry.py")
}

resource "aws_s3_object" "pkg" {
  bucket = aws_s3_bucket.zones["scripts"].id
  key    = "glue_jobs.zip"
  source = "${path.module}/../build/glue_jobs.zip"
  etag   = filemd5("${path.module}/../build/glue_jobs.zip")
}

resource "aws_s3_object" "rules" {
  bucket = aws_s3_bucket.zones["scripts"].id
  key    = "rules.yaml"
  source = "${path.module}/../dq/rules.yaml"
  etag   = filemd5("${path.module}/../dq/rules.yaml")
}

resource "aws_glue_job" "clean_daily" {
  name              = local.job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2  # the minimum
  timeout           = 10 # minutes: caps a runaway job
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.zones["scripts"].bucket}/clean_daily_entry.py"
  }

  default_arguments = {
    "--extra-py-files"            = "s3://${aws_s3_bucket.zones["scripts"].bucket}/glue_jobs.zip"
    "--additional-python-modules" = "pyyaml"
    "--TempDir"                   = "s3://${aws_s3_bucket.zones["scripts"].bucket}/tmp/"
    "--ENV"                       = "glue"
    "--RAW_URI"                   = "s3://${aws_s3_bucket.zones["raw"].bucket}"
    "--CLEAN_URI"                 = "s3://${aws_s3_bucket.zones["clean"].bucket}"
    "--QUARANTINE_URI"            = "s3://${aws_s3_bucket.zones["quarantine"].bucket}"
    "--RULES_PATH"                = "s3://${aws_s3_bucket.zones["scripts"].bucket}/rules.yaml"
    "--WATERMARK_BACKEND"         = "dynamo"
    "--WATERMARK_TABLE"           = aws_dynamodb_table.watermarks.name
  }

  depends_on = [
    aws_iam_role_policy.glue,
    aws_s3_object.entry,
    aws_s3_object.pkg,
    aws_s3_object.rules,
  ]
}