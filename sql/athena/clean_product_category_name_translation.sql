CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.clean_product_category_name_translation (
  `product_category_name` string,
  `product_category_name_english` string)
PARTITIONED BY (ingest_dt string)
STORED AS PARQUET
LOCATION 's3://cartflow-28cddc-clean/product_category_name_translation/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_dt.type'='date',
  'projection.ingest_dt.range'='2017-11-01,2017-12-31',
  'projection.ingest_dt.format'='yyyy-MM-dd',
  'storage.location.template'='s3://cartflow-28cddc-clean/product_category_name_translation/ingest_dt=${ingest_dt}/')
