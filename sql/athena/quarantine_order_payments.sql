CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.quarantine_order_payments (
  `order_id` string,
  `payment_sequential` int,
  `payment_type` string,
  `payment_installments` int,
  `payment_value` double,
  `dq_warn` string,
  `dq_reason` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-quarantine/order_payments/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-quarantine/order_payments/ingest_dt=${ingest_dt}/')
