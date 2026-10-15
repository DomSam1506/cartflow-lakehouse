"""Write sql/athena/raw_*.sql with the real raw bucket name filled in.

    python scripts/make_raw_ddl.py --raw-bucket "$RAW_BUCKET"
"""
import argparse
from pathlib import Path

TEMPLATES = Path("sql/athena")
PROJ = ("TBLPROPERTIES ('projection.enabled'='true', 'projection.dt.type'='date', "
        "'projection.dt.range'='2017-11-20,2017-12-01', 'projection.dt.format'='yyyy-MM-dd', "
        "'storage.location.template'='s3://{b}/dt=${{dt}}/{t}/'{extra})")

EVENTS = """CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.raw_events (
  event_id string, session_id string, customer_unique_id string, product_id string,
  event_type string, event_ts string, device string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION 's3://{b}/'
""" + PROJ

ORDERS = """CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.raw_orders (
  order_id string, customer_id string, order_status string, order_purchase_timestamp string,
  order_approved_at string, order_delivered_carrier_date string,
  order_delivered_customer_date string, order_estimated_delivery_date string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://{b}/'
""" + PROJ

ITEMS = """CREATE EXTERNAL TABLE IF NOT EXISTS cartflow.raw_order_items (
  order_id string, order_item_id string, product_id string, seller_id string,
  shipping_limit_date string, price string, freight_value string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://{b}/'
""" + PROJ

SKIP = ", 'skip.header.line.count'='1'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-bucket", required=True)
    b = ap.parse_args().raw_bucket
    TEMPLATES.mkdir(parents=True, exist_ok=True)
    for name, ddl, table, extra in [("raw_events", EVENTS, "events", ""),
                                    ("raw_orders", ORDERS, "orders", SKIP),
                                    ("raw_order_items", ITEMS, "order_items", SKIP)]:
        sql = ddl.format(b=b, t=table, extra=extra) + ";\n"
        (TEMPLATES / f"{name}.sql").write_text(sql)
        print("wrote", TEMPLATES / f"{name}.sql")


if __name__ == "__main__":
    main()