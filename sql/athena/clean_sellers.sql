CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.clean_sellers (
  `seller_id` string,
  `seller_zip_code_prefix` string,
  `seller_city` string,
  `seller_state` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-clean/sellers/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-clean/sellers/ingest_dt=${ingest_dt}/')
