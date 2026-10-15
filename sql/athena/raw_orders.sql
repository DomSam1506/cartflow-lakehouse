CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.raw_orders (
  order_id string, customer_id string, order_status string, order_purchase_timestamp string,
  order_approved_at string, order_delivered_carrier_date string,
  order_delivered_customer_date string, order_estimated_delivery_date string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://cartflow-28cddc-raw/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.dt.type'='date', 'projection.dt.range'='2017-11-20,2017-12-01', 'projection.dt.format'='yyyy-MM-dd', 'storage.location.template'='s3://cartflow-28cddc-raw/dt=${dt}/orders/', 'skip.header.line.count'='1');
