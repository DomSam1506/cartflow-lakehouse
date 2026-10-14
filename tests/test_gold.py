"""Unit tests for the gold builders on tiny hand-made DataFrames (local Spark, no files)."""
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from glue_jobs.clean_daily import build_spark
from glue_jobs.common import gold


@pytest.fixture(scope="module")
def spark():
    return build_spark("local")  # session time zone is UTC, as in the real job


def frame(spark, rows, schema, ts_cols=()):
    """Timestamps come in as strings and are cast in the UTC session, so the tests do not
    depend on the time zone of the machine they run on."""
    df = spark.createDataFrame(rows, schema)
    for c in ts_cols:
        df = df.withColumn(c, F.col(c).cast("timestamp"))
    return df


EVENT_SCHEMA = ("event_id string, session_id string, customer_unique_id string, "
                "event_type string, event_ts string, device string, ingest_dt string")


def events(spark, rows):
    return frame(spark, rows, EVENT_SCHEMA, ["event_ts"])


def by_session(df):
    return {r["session_id"]: r for r in df.collect()}


def test_session_funnel_flags_and_event_dedupe(spark):
    ev = events(spark, [
        ("e1", "s1", "u1", "page_view", "2017-11-24 10:00:00", "mobile", "2017-11-24"),
        ("e2", "s1", "u1", "add_to_cart", "2017-11-24 10:01:00", "mobile", "2017-11-24"),
        ("e3", "s1", "u1", "checkout_start", "2017-11-24 10:02:00", "mobile", "2017-11-24"),
        ("e4", "s1", "u1", "purchase", "2017-11-24 10:03:00", "mobile", "2017-11-24"),
        ("e5", "s2", "u2", "page_view", "2017-11-24 11:00:00", "desktop", "2017-11-24"),
        # s3 lost its cart and checkout events to quarantine but still purchased
        ("e6", "s3", "u3", "page_view", "2017-11-24 12:00:00", "desktop", "2017-11-24"),
        ("e7", "s3", "u3", "purchase", "2017-11-24 12:05:00", "desktop", "2017-11-24"),
        # e1 shipped again the next day: a late duplicate
        ("e1", "s1", "u1", "page_view", "2017-11-24 10:00:00", "mobile", "2017-11-25"),
    ])
    s = by_session(gold.build_sessions(ev, "2017-11-24"))
    assert set(s) == {"s1", "s2", "s3"}
    assert s["s1"]["n_events"] == 4 and s["s1"]["furthest_stage"] == "purchase"
    assert s["s2"]["viewed"] and not s["s2"]["carted"] and s["s2"]["furthest_stage"] == "page_view"
    assert s["s3"]["carted"] and s["s3"]["checkout_started"] and s["s3"]["purchased"]  # "reached" semantics
    assert str(s["s1"]["session_date"]) == "2017-11-24"


def test_a_late_event_completes_an_earlier_session_from_history(spark):
    ev = events(spark, [
        ("e1", "s1", "u1", "page_view", "2017-11-24 10:00:00", "mobile", "2017-11-24"),
        ("e2", "s2", "u2", "page_view", "2017-11-24 11:00:00", "mobile", "2017-11-24"),
        ("e3", "s2", "u2", "add_to_cart", "2017-11-24 11:02:00", "mobile", "2017-11-25"),  # late
        ("e1", "s1", "u1", "page_view", "2017-11-24 10:00:00", "mobile", "2017-11-25"),    # late duplicate
    ])
    df = gold.build_sessions(ev, "2017-11-25").withColumn(
        "start_text", F.date_format("session_start", "yyyy-MM-dd HH:mm:ss"))  # formatted in Spark (UTC)
    s = by_session(df)
    assert set(s) == {"s2"}  # only sessions touched by new data; the duplicate does not touch s1
    assert s["s2"]["n_events"] == 2 and s["s2"]["carted"]
    assert s["s2"]["start_text"] == "2017-11-24 11:00:00"  # history was included


ORDER_SCHEMA = ("order_id string, customer_id string, order_status string, order_purchase_timestamp string, "
                "order_approved_at string, order_delivered_carrier_date string, "
                "order_delivered_customer_date string, order_estimated_delivery_date string")
ORDER_TS = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date"]
SNAP_SCHEMA = "customer_id string, customer_unique_id string, zip string, city string, state string, effective_date string"


def orders(spark, rows):
    return frame(spark, rows, ORDER_SCHEMA, ORDER_TS)


def snapshots(spark, rows):
    return frame(spark, rows, SNAP_SCHEMA)


O1 = ("o1", "c1", "delivered", "2017-11-25 01:30:00", "2017-11-25 02:00:00", "2017-11-26 00:00:00",
      "2017-12-10 00:00:00", "2017-12-01 00:00:00")
O2 = ("o2", "c2", "canceled", "2017-11-24 15:00:00", None, None, None, "2017-12-05 00:00:00")


def test_fact_orders_totals_dates_and_lateness(spark):
    items = spark.createDataFrame([
        ("o1", 1, "p1", "s1", Decimal("10.00"), Decimal("2.00")),
        ("o1", 2, "p2", "s1", Decimal("5.50"), Decimal("1.00")),
    ], "order_id string, order_item_id int, product_id string, seller_id string, "
       "price decimal(10,2), freight_value decimal(10,2)")
    pays = spark.createDataFrame([("o1", Decimal("18.50"))], "order_id string, payment_value decimal(10,2)")
    snaps = snapshots(spark, [("c1", "u1", "1046", "sao paulo", "SP", "2017-11-24"),
                              ("c2", "u2", "20000", "rio", "RJ", "2017-11-24")])
    got = {r["order_id"]: r for r in gold.build_fact_orders(orders(spark, [O1, O2]), items, pays, snaps).collect()}
    o1, o2 = got["o1"], got["o2"]
    assert str(o1["order_date"]) == "2017-11-24"  # 01:30 UTC is still the evening of the 24th in Brazil
    assert (o1["n_items"], o1["items_value"], o1["freight_value"], o1["payment_value"]) == (
        2, Decimal("15.50"), Decimal("3.00"), Decimal("18.50"))
    assert o1["delivered_late"] is True and o1["customer_unique_id"] == "u1"
    assert (o2["n_items"], o2["items_value"], o2["payment_value"]) == (0, Decimal("0.00"), Decimal("0.00"))
    assert o2["delivered_late"] is None  # not delivered yet


def test_fact_items_carry_the_person_and_the_brazil_date(spark):
    items = spark.createDataFrame([("o1", 1, "p1", "s1", Decimal("10.00"), Decimal("2.00"))],
                                  "order_id string, order_item_id int, product_id string, seller_id string, "
                                  "price decimal(10,2), freight_value decimal(10,2)")
    snaps = snapshots(spark, [("c1", "u1", "1046", "sao paulo", "SP", "2017-11-24")])
    row = gold.build_fact_items(orders(spark, [O1]), items, snaps).collect()[0]
    assert row["customer_unique_id"] == "u1" and str(row["order_date"]) == "2017-11-24"


def test_an_order_with_no_known_person_fails_loudly(spark):
    snaps = snapshots(spark, [("someone-else", "u9", "1046", "x", "SP", "2017-11-24")])
    with pytest.raises(ValueError, match="no customer snapshot"):
        gold.attach_customer(orders(spark, [O1]), snaps)


def test_customer_feed_keeps_the_latest_address_per_person_per_day(spark):
    snaps = snapshots(spark, [("c1", "u1", "1046", "sao paulo", "SP", "2017-11-24"),
                              ("c2", "u1", "20000", "rio", "RJ", "2017-11-24")])
    ords = frame(spark, [("c1", "2017-11-24 10:00:00"), ("c2", "2017-11-24 18:00:00")],
                 "customer_id string, order_purchase_timestamp string", ["order_purchase_timestamp"])
    rows = gold.build_customer_feed(snaps, ords).collect()
    assert len(rows) == 1
    assert (rows[0]["zip"], rows[0]["city"]) == ("20000", "rio")  # the later order wins


def test_customer_feed_restores_leading_zeros(spark):
    snaps = snapshots(spark, [("c1", "u1", "1046", "sao paulo", "SP", "2017-11-24")])
    ords = frame(spark, [("c1", "2017-11-24 10:00:00")],
                 "customer_id string, order_purchase_timestamp string", ["order_purchase_timestamp"])
    assert gold.build_customer_feed(snaps, ords).collect()[0]["zip"] == "01046"


def test_dim_product_translates_categories_with_fallbacks(spark):
    products = spark.createDataFrame([
        ("p1", "beleza_saude", 10, 100, 1, 500, 10, 10, 10),
        ("p2", "xyz_untranslated", 10, 100, 1, 500, 10, 10, 10),
        ("p3", None, None, None, None, None, None, None, None),
    ], "product_id string, product_category_name string, product_name_lenght int, "
       "product_description_lenght int, product_photos_qty int, product_weight_g int, "
       "product_length_cm int, product_height_cm int, product_width_cm int")
    translation = spark.createDataFrame([("beleza_saude", "health_beauty")],
                                        "product_category_name string, product_category_name_english string")
    got = {r["product_id"]: r["category_en"] for r in gold.build_dim_product(products, translation).collect()}
    assert got == {"p1": "health_beauty", "p2": "xyz_untranslated", "p3": "unknown"}