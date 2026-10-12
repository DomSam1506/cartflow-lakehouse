CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.quarantine_events (
  `event_id` string,
  `session_id` string,
  `customer_unique_id` string,
  `product_id` string,
  `event_type` string,
  `event_ts` timestamp,
  `device` string,
  `event_ts_raw` string,
  `_corrupt_record` string,
  `dq_warn` string,
  `dq_reason` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-quarantine/events/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-quarantine/events/ingest_dt=${ingest_dt}/')
