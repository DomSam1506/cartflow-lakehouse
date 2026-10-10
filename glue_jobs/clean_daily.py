"""Clean one day of raw data: read -> flag bad rows -> UTC -> write clean + quarantine.

python -m glue_jobs.clean_daily --dt 2017-11-24
"""

import argparse
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from glue_jobs.common.dq import apply_rules, load_rules, summarize
from glue_jobs.common.schema import SCHEMAS, read_table
from glue_jobs.common.transforms import dedupe, parse_ts, to_utc
from glue_jobs.common.watermark import LocalWatermarks, advance

TABLES = ["events", *SCHEMAS]
HELPER_COLS = ["_corrupt_record", "event_ts_raw"]
DEDUPE_KEYS = {"order_reviews": ["review_id", "order_id"]}


def build_spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("cartflow-clean")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def prepare(df, name):
    """Parse event timestamps, then convert every timestamp column Brazil local -> UTC."""
    if name == "events":
        df = parse_ts(df, "event_ts")
    for f in df.schema.fields:
        if isinstance(f.dataType, TimestampType):
            df = to_utc(df, f.name)
    return df


def write_partition(df, base, dt):
    """Idempotent write of one ingest_dt partition. Returns the row count."""
    n = df.count()
    if n == 0:
        # Dynamic overwrite writes nothing for an empty frame, which would leave the
        # previous run's partition in place. Remove it explicitly.
        shutil.rmtree(os.path.join(base, f"ingest_dt={dt}"), ignore_errors=True)
        return 0
    df.write.mode("overwrite").partitionBy("ingest_dt").parquet(base)
    return n


def process_table(spark, name, dt, src, lake_dir, rules, watermarks):
    df = prepare(read_table(spark, name, src), name).cache()
    total = df.count()

    if name in rules:
        valid, bad = apply_rules(df, rules[name])
    else:
        valid, bad = df, df.limit(0).withColumn("dq_reason", F.lit(""))

    dups_dropped = 0
    if name in DEDUPE_KEYS:
        before = valid.count()
        valid = dedupe(valid, DEDUPE_KEYS[name])
        dups_dropped = before - valid.count()

    valid = (
        valid.drop(*[c for c in HELPER_COLS if c in valid.columns])
        .withColumn("ingest_dt", F.lit(dt))
        .cache()
    )
    bad = bad.withColumn("ingest_dt", F.lit(dt))

    warned = valid.filter("dq_warn != ''").count() if "dq_warn" in valid.columns else 0
    summary = summarize(name, total, bad)

    clean_n = write_partition(valid, os.path.join(lake_dir, "clean", name), dt)
    write_partition(bad, os.path.join(lake_dir, "quarantine", name), dt)

    # Only now, after BOTH writes succeeded, move the watermark.
    moved = advance(watermarks, name, dt)

    summary.update(
        clean=clean_n, warned=warned, dups_dropped=dups_dropped, watermark_moved=moved
    )
    return summary


def run(
    spark,
    dt,
    raw_dir="data/local_lake/raw",
    lake_dir="data/local_lake",
    rules_path="dq/rules.yaml",
    watermarks=None,
    tables=None,
):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    rules = load_rules(rules_path)
    watermarks = watermarks or LocalWatermarks()
    results = []
    for name in tables or TABLES:
        src = f"{raw_dir}/dt={dt}/{name}/"
        if not os.path.isdir(src):
            print(f"{dt} {name}: not in this day's drop, skipping")
            continue
        results.append(process_table(spark, name, dt, src, lake_dir, rules, watermarks))
        spark.catalog.clearCache()
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True, help="ingest day, YYYY-MM-DD")
    ap.add_argument("--raw", default="data/local_lake/raw")
    ap.add_argument("--lake", default="data/local_lake")
    ap.add_argument("--watermarks", default="data/local_watermarks.json")
    a = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    results = run(spark, a.dt, a.raw, a.lake, watermarks=LocalWatermarks(a.watermarks))

    print(f"\n=== clean_daily {a.dt} ===")
    print(
        f"{'table':<36}{'total':>8}{'clean':>8}{'quarantined':>13}{'pct':>8}{'warned':>8}"
    )
    for r in results:
        print(
            f"{r['table']:<36}{r['total']:>8,}{r['clean']:>8,}"
            f"{r['quarantined']:>13,}{r['pct']:>7.2f}%{r['warned']:>8,}"
        )
        if r["by_reason"]:
            print(f"    reasons: {r['by_reason']}")
        if r["dups_dropped"]:
            print(f"    duplicates dropped: {r['dups_dropped']}")


if __name__ == "__main__":
    main()
