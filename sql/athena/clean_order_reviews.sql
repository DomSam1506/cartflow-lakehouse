CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.clean_order_reviews (
  `review_id` string,
  `order_id` string,
  `review_score` int,
  `review_comment_title` string,
  `review_comment_message` string,
  `review_creation_date` timestamp,
  `review_answer_timestamp` timestamp,
  `dq_warn` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-clean/order_reviews/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-clean/order_reviews/ingest_dt=${ingest_dt}/')
