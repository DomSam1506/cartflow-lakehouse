CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.raw_order_items (
  order_id string, order_item_id string, product_id string, seller_id string,
  shipping_limit_date string, price string, freight_value string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://cartflow-28cddc-raw/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.dt.type'='date', 'projection.dt.range'='2017-11-20,2017-12-01', 'projection.dt.format'='yyyy-MM-dd', 'storage.location.template'='s3://cartflow-28cddc-raw/dt=${dt}/order_items/', 'skip.header.line.count'='1');
