"""Clean one day of raw data: read -> flag bad rows -> UTC -> write clean + quarantine.

Locally:   python -m glue_jobs.clean_daily --dt 2017-11-24
On Glue:   glue_entry.py calls main(); Terraform supplies the arguments.
Paths may be local folders or s3:// URIs.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from glue_jobs.common import storage
from glue_jobs.common.dq import apply_rules, load_rules, summarize
from glue_jobs.common.schema import SCHEMAS, read_table
from glue_jobs.common.transforms import dedupe, parse_ts, to_utc
from glue_jobs.common.watermark import DynamoWatermarks, LocalWatermarks, advance

TABLES = ["events", *SCHEMAS]
HELPER_COLS = ["_corrupt_record", "event_ts_raw"]
DEDUPE_KEYS = {"order_reviews": ["review_id", "order_id"]}


def configure(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # millisecond timestamps are read cleanly by Athena
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")


def build_spark(env="local"):
    builder = SparkSession.builder.appName("cartflow-clean")
    if env == "local":
        builder = (
            builder.master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
        )
    spark = (
        builder.getOrCreate()
    )  # on Glue this returns the session Glue already started
    configure(spark)
    return spark


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
        # previous run's partition in place. Remove it explicitly (local folder or S3 prefix).
        storage.delete_prefix(storage.join(base, f"ingest_dt={dt}"))
        return 0
    df.write.mode("overwrite").partitionBy("ingest_dt").parquet(base)
    return n


def process_table(spark, name, dt, src, clean_dir, quarantine_dir, rules, watermarks):
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

    clean_n = write_partition(valid, storage.join(clean_dir, name), dt)
    write_partition(bad, storage.join(quarantine_dir, name), dt)

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
    clean_dir=None,
    quarantine_dir=None,
):
    configure(spark)
    clean_dir = clean_dir or storage.join(lake_dir, "clean")
    quarantine_dir = quarantine_dir or storage.join(lake_dir, "quarantine")
    rules = load_rules(rules_path)
    watermarks = watermarks or LocalWatermarks()
    results = []
    for name in tables or TABLES:
        src = storage.join(raw_dir, f"dt={dt}", name) + "/"
        if not storage.exists_dir(src):
            print(f"{dt} {name}: not in this day's drop, skipping")
            continue
        results.append(
            process_table(
                spark, name, dt, src, clean_dir, quarantine_dir, rules, watermarks
            )
        )
        spark.catalog.clearCache()
    return results


def parse_args(argv=None):
    # Glue passes extra arguments of its own (--JOB_NAME, --TempDir ...), so ignore unknowns.
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--dt", required=True, help="ingest day, YYYY-MM-DD")
    ap.add_argument("--ENV", dest="env", choices=["local", "glue"], default="local")
    ap.add_argument("--lake", default="data/local_lake")
    ap.add_argument("--RAW_URI", "--raw", dest="raw")
    ap.add_argument("--CLEAN_URI", "--clean", dest="clean")
    ap.add_argument("--QUARANTINE_URI", "--quarantine", dest="quarantine")
    ap.add_argument("--RULES_PATH", "--rules", dest="rules", default="dq/rules.yaml")
    ap.add_argument(
        "--WATERMARK_BACKEND",
        "--watermark-backend",
        dest="wm_backend",
        choices=["local", "dynamo"],
        default="local",
    )
    ap.add_argument(
        "--WATERMARK_TABLE",
        "--watermark-table",
        dest="wm_table",
        default="cartflow-watermarks",
    )
    ap.add_argument("--watermarks", default="data/local_watermarks.json")
    return ap.parse_known_args(argv)[0]


def main(argv=None):
    a = parse_args(argv)
    spark = build_spark(a.env)
    if a.env == "local":
        spark.sparkContext.setLogLevel("ERROR")
    wm = (
        DynamoWatermarks(a.wm_table)
        if a.wm_backend == "dynamo"
        else LocalWatermarks(a.watermarks)
    )

    results = run(
        spark,
        a.dt,
        raw_dir=a.raw or storage.join(a.lake, "raw"),
        lake_dir=a.lake,
        rules_path=a.rules,
        watermarks=wm,
        clean_dir=a.clean,
        quarantine_dir=a.quarantine,
    )

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
