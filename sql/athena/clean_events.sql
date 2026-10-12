CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.clean_events (
  `event_id` string,
  `session_id` string,
  `customer_unique_id` string,
  `product_id` string,
  `event_type` string,
  `event_ts` timestamp,
  `device` string,
  `dq_warn` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-clean/events/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-clean/events/ingest_dt=${ingest_dt}/')
