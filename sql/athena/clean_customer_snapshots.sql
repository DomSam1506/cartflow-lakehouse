CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.clean_customer_snapshots (
  `customer_unique_id` string,
  `zip` string,
  `city` string,
  `state` string,
  `effective_date` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-clean/customer_snapshots/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-clean/customer_snapshots/ingest_dt=${ingest_dt}/')
