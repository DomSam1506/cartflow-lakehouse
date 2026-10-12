output "raw_bucket" { value = aws_s3_bucket.zones["raw"].bucket }
output "clean_bucket" { value = aws_s3_bucket.zones["clean"].bucket }
output "quarantine_bucket" { value = aws_s3_bucket.zones["quarantine"].bucket }
output "glue_job" { value = aws_glue_job.clean_daily.name }