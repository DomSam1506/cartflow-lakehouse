from pyspark.sql import functions as F

from glue_jobs.common.dq import apply_rules, load_rules, summarize
from glue_jobs.common.transforms import parse_ts

EVENT_RULES = {"reject": {
    "not_null": ["event_id", "event_ts"],
    "unique": ["event_id"],
    "allowed_values": {"event_type": ["page_view", "purchase"]},
    "valid_timestamp": ["event_ts"],
    "ts_range": {"column": "event_ts", "min": "2016-01-01", "max": "2019-01-01"},
}}


def ev(event_id="e1", etype="page_view", ts="2017-11-24T10:00:00"):
    return (event_id, etype, ts)


def events_df(spark, rows):
    df = spark.createDataFrame(rows, "event_id string, event_type string, event_ts string")
    return parse_ts(df, "event_ts")


def reasons(bad, key="event_type"):
    return {r[key]: r["dq_reason"] for r in bad.select(key, "dq_reason").collect()}


def test_null_event_id_is_quarantined_with_reason(spark):
    valid, bad = apply_rules(events_df(spark, [ev("e1"), ev(None, "purchase")]), EVENT_RULES)
    assert valid.count() == 1
    assert reasons(bad) == {"purchase": "null:event_id"}


def test_bad_event_type(spark):
    _, bad = apply_rules(events_df(spark, [ev("e1", "page_viw")]), EVENT_RULES)
    assert reasons(bad) == {"page_viw": "bad_value:event_type"}


def test_duplicate_event_id_only_extra_copies_are_flagged(spark):
    valid, bad = apply_rules(events_df(spark, [ev("e1"), ev("e1"), ev("e2")]), EVENT_RULES)
    assert valid.count() == 2
    assert bad.count() == 1
    assert bad.first().dq_reason == "duplicate:event_id"


def test_bad_and_ancient_timestamps(spark):
    df = events_df(spark, [ev("e1", ts="not-a-timestamp"), ev("e2", ts="1970-01-01T00:00:00")])
    _, bad = apply_rules(df, EVENT_RULES)
    got = reasons(bad, "event_id")
    assert "bad_timestamp:event_ts" in got["e1"]
    assert got["e2"] == "out_of_range:event_ts"


def test_nothing_is_lost(spark):
    df = events_df(spark, [ev("e1"), ev("e1"), ev(None), ev("e3", "x"), ev("e4", ts="bad")])
    valid, bad = apply_rules(df, EVENT_RULES)
    assert valid.count() + bad.count() == df.count()


def test_summary_percentage(spark):
    df = events_df(spark, [ev("e1"), ev("e2"), ev("e3"), ev(None)])
    _, bad = apply_rules(df, EVENT_RULES)
    s = summarize("events", df.count(), bad)
    assert (s["total"], s["quarantined"], s["pct"]) == (4, 1, 25.0)
    assert s["by_reason"] == {"null:event_id": 1}


ORDER_RULES = {
    "reject": {"required_when": [{"column": "delivered_at", "when_column": "status",
                                  "equals": "delivered"}]},
    "warn": {"date_order": [{"earlier": "approved_at", "later": "carrier_at"}]},
}


def orders_df(spark, rows):
    df = spark.createDataFrame(rows, "id string, status string, approved_at string, "
                                     "carrier_at string, delivered_at string")
    for c in ["approved_at", "carrier_at", "delivered_at"]:
        df = df.withColumn(c, F.to_timestamp(c))
    return df


def test_delivered_without_date_is_rejected_but_shipped_is_fine(spark):
    df = orders_df(spark, [("o1", "delivered", None, None, None),
                           ("o2", "shipped", None, None, None)])
    valid, bad = apply_rules(df, ORDER_RULES)
    assert [r.id for r in valid.collect()] == ["o2"]
    assert bad.first().dq_reason == "missing_when:delivered_at:status=delivered"


def test_warn_keeps_the_row_and_flags_it(spark):
    df = orders_df(spark, [("o3", "shipped", "2017-11-24 10:00:00", "2017-11-24 09:00:00", None)])
    valid, bad = apply_rules(df, ORDER_RULES)
    assert bad.count() == 0
    assert valid.first().dq_warn == "date_order:carrier_at<approved_at"


def test_negative_price_gets_non_positive_reason(spark):
    df = spark.createDataFrame([("o1", -1.0), ("o2", None), ("o3", 5.0)], "id string, price double")
    rules = {"reject": {"not_null": ["price"], "positive": ["price"]}}
    valid, bad = apply_rules(df, rules)
    got = reasons(bad, "id")
    assert got == {"o1": "non_positive:price", "o2": "null:price"}
    assert valid.count() == 1


def test_real_rules_file_loads():
    rules = load_rules("dq/rules.yaml")
    assert {"events", "orders", "order_items", "order_payments"} <= set(rules)