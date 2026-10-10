from pyspark.sql import functions as F

from glue_jobs.common.transforms import dedupe, parse_ts, to_utc


def test_dedupe_removes_duplicate_event_ids(spark):
    df = spark.createDataFrame(
        [("e1", "a"), ("e1", "a"), ("e2", "b")], ["event_id", "x"]
    )
    out = dedupe(df, "event_id")
    assert out.count() == 2
    assert out.select("event_id").distinct().count() == 2


def test_parse_ts_keeps_raw_and_nulls_bad_values(spark):
    df = spark.createDataFrame(
        [("2017-11-24T10:00:00",), ("not-a-timestamp",)], ["event_ts"]
    )
    rows = {r.event_ts_raw: r.event_ts for r in parse_ts(df, "event_ts").collect()}
    assert rows["2017-11-24T10:00:00"] is not None
    assert rows["not-a-timestamp"] is None


def _utc_text(df):
    return df.select(F.date_format("ts", "yyyy-MM-dd HH:mm:ss").alias("t")).first().t


def test_to_utc_handles_brazil_daylight_saving(spark):
    # Nov 2017: Brazil was on summer time (UTC-2). June 2017: UTC-3.
    nov = spark.createDataFrame([("2017-11-24 12:00:00",)], ["ts"]).withColumn(
        "ts", F.to_timestamp("ts")
    )
    jun = spark.createDataFrame([("2017-06-15 12:00:00",)], ["ts"]).withColumn(
        "ts", F.to_timestamp("ts")
    )
    assert _utc_text(to_utc(nov, "ts")) == "2017-11-24 14:00:00"
    assert _utc_text(to_utc(jun, "ts")) == "2017-06-15 15:00:00"
