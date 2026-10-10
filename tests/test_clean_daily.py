import json

import pytest

from glue_jobs import clean_daily
from glue_jobs.common.watermark import LocalWatermarks

ORDER_HEADER = (
    "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
    "order_delivered_carrier_date,order_delivered_customer_date,"
    "order_estimated_delivery_date"
)


def ev(event_id, etype="page_view", ts="2017-11-24T10:00:00"):
    return {
        "event_id": event_id,
        "session_id": "s1",
        "customer_unique_id": "u1",
        "product_id": "p1",
        "event_type": etype,
        "event_ts": ts,
        "device": "mobile",
    }


def write_day(tmp, dt, events=None, orders=None):
    raw = tmp / "raw" / f"dt={dt}"
    if events is not None:
        d = raw / "events"
        d.mkdir(parents=True, exist_ok=True)
        (d / "part-0.json").write_text("\n".join(json.dumps(e) for e in events) + "\n")
    if orders is not None:
        d = raw / "orders"
        d.mkdir(parents=True, exist_ok=True)
        (d / "part-0.csv").write_text(ORDER_HEADER + "\n" + "\n".join(orders) + "\n")


def run_day(spark, tmp, dt, tables=("events",)):
    return clean_daily.run(
        spark,
        dt,
        raw_dir=str(tmp / "raw"),
        lake_dir=str(tmp / "lake"),
        watermarks=LocalWatermarks(str(tmp / "wm.json")),
        tables=list(tables),
    )


def count(spark, tmp, zone, table):
    path = tmp / "lake" / zone / table
    return spark.read.parquet(str(path)).count() if path.exists() else 0


def test_double_run_changes_nothing(spark, tmp_path):
    write_day(
        tmp_path,
        "2017-11-24",
        events=[ev("e1"), ev("e2"), ev("e2"), ev(None)],
        orders=["o1,c1,shipped,2017-11-24 10:00:00,,,,2017-12-01 00:00:00"],
    )

    def snapshot():
        return {
            (z, t): count(spark, tmp_path, z, t)
            for z in ("clean", "quarantine")
            for t in ("events", "orders")
        }

    run_day(spark, tmp_path, "2017-11-24", tables=("events", "orders"))
    first = snapshot()
    run_day(spark, tmp_path, "2017-11-24", tables=("events", "orders"))
    assert snapshot() == first
    assert first[("clean", "events")] == 2  # e1, and one copy of e2
    assert first[("quarantine", "events")] == 2  # duplicate e2, null id
    assert first[("clean", "orders")] == 1


def test_job_quarantines_with_the_right_reason(spark, tmp_path):
    write_day(tmp_path, "2017-11-24", events=[ev("e1", "page_viw")])
    run_day(spark, tmp_path, "2017-11-24")
    bad = spark.read.parquet(str(tmp_path / "lake/quarantine/events")).first()
    assert bad.dq_reason == "bad_value:event_type"


def test_rerun_removes_stale_quarantine_when_data_is_now_clean(spark, tmp_path):
    write_day(tmp_path, "2017-11-24", events=[ev("e1", "page_viw")])
    run_day(spark, tmp_path, "2017-11-24")
    part = tmp_path / "lake/quarantine/events/ingest_dt=2017-11-24"
    assert part.exists()

    write_day(tmp_path, "2017-11-24", events=[ev("e1")])  # the bad row is fixed
    run_day(spark, tmp_path, "2017-11-24")
    assert not part.exists()  # no stale leftovers
    assert count(spark, tmp_path, "clean", "events") == 1


def test_late_event_does_not_overwrite_the_earlier_day(spark, tmp_path):
    write_day(
        tmp_path,
        "2017-11-23",
        events=[ev("a", ts="2017-11-23T10:00:00"), ev("b", ts="2017-11-23T11:00:00")],
    )
    write_day(
        tmp_path, "2017-11-24", events=[ev("c", ts="2017-11-23T23:00:00")]
    )  # late
    run_day(spark, tmp_path, "2017-11-23")
    run_day(spark, tmp_path, "2017-11-24")
    df = spark.read.parquet(str(tmp_path / "lake/clean/events"))
    assert df.filter("ingest_dt = '2017-11-23'").count() == 2  # untouched
    assert df.filter("ingest_dt = '2017-11-24'").count() == 1


def test_watermark_does_not_advance_when_a_write_fails(spark, tmp_path, monkeypatch):
    write_day(tmp_path, "2017-11-24", events=[ev("e1"), ev(None)])
    real, calls = clean_daily.write_partition, {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] == 2:  # clean write ok, quarantine write dies
            raise RuntimeError("boom")
        return real(*a, **k)

    monkeypatch.setattr(clean_daily, "write_partition", flaky)
    with pytest.raises(RuntimeError):
        run_day(spark, tmp_path, "2017-11-24")
    assert LocalWatermarks(str(tmp_path / "wm.json")).get("events") is None

    monkeypatch.setattr(clean_daily, "write_partition", real)  # retry succeeds
    run_day(spark, tmp_path, "2017-11-24")
    assert LocalWatermarks(str(tmp_path / "wm.json")).get("events") == "2017-11-24"


def test_watermark_never_moves_backwards(spark, tmp_path):
    write_day(tmp_path, "2017-11-24", events=[ev("e1")])
    write_day(tmp_path, "2017-11-25", events=[ev("e2")])
    run_day(spark, tmp_path, "2017-11-25")
    run_day(spark, tmp_path, "2017-11-24")  # re-running an older day
    assert LocalWatermarks(str(tmp_path / "wm.json")).get("events") == "2017-11-25"
