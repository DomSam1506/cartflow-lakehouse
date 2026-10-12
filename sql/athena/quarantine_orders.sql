CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.quarantine_orders (
  `order_id` string,
  `customer_id` string,
  `order_status` string,
  `order_purchase_timestamp` timestamp,
  `order_approved_at` timestamp,
  `order_delivered_carrier_date` timestamp,
  `order_delivered_customer_date` timestamp,
  `order_estimated_delivery_date` timestamp,
  `dq_warn` string,
  `dq_reason` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-quarantine/orders/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-quarantine/orders/ingest_dt=${ingest_dt}/')
