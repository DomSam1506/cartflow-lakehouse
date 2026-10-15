CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.raw_events (
  event_id string, session_id string, customer_unique_id string, product_id string,
  event_type string, event_ts string, device string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION 's3://cartflow-28cddc-raw/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.dt.type'='date', 'projection.dt.range'='2017-11-20,2017-12-01', 'projection.dt.format'='yyyy-MM-dd', 'storage.location.template'='s3://cartflow-28cddc-raw/dt=${dt}/events/');
