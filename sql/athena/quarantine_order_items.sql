CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.quarantine_order_items (
  `order_id` string,
  `order_item_id` int,
  `product_id` string,
  `seller_id` string,
  `shipping_limit_date` timestamp,
  `price` double,
  `freight_value` double,
  `dq_warn` string,
  `dq_reason` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-quarantine/order_items/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-quarantine/order_items/ingest_dt=${ingest_dt}/')
