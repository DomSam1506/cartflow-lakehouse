"""Build the gold layer for ONE ingest day from the clean zone.

    python -m glue_jobs.build_gold --dt 2017-11-24
    python -m glue_jobs.build_gold --dt 2017-11-24 --lake data/local_lake

Output: <gold>/<table>/dt=<dt>/part-*.parquet   (re-running a day replaces that folder)
"""
import argparse

from pyspark.sql import functions as F

from glue_jobs.clean_daily import build_spark, configure
from glue_jobs.common import gold, storage

DAY_TABLES = ["orders", "order_items", "order_payments", "customer_snapshots",
              "products", "sellers", "product_category_name_translation"]


def read_day(spark, clean, table, dt):
    """One ingest_dt partition of a clean table, or None if that day shipped none of it."""
    base = storage.join(clean, table)
    part = storage.join(base, f"ingest_dt={dt}")
    if not storage.exists_dir(part):
        return None
    return spark.read.option("basePath", base).parquet(part).withColumn(
        "ingest_dt", F.col("ingest_dt").cast("string"))


def read_events_upto(spark, clean, dt):
    base = storage.join(clean, "events")
    if not storage.exists_dir(storage.join(base, f"ingest_dt={dt}")):
        return None
    ev = spark.read.parquet(base).withColumn("ingest_dt", F.col("ingest_dt").cast("string"))
    return ev.filter(F.col("ingest_dt") <= dt)


def write_gold(df, gold_dir, table, dt):
    """Replace this day's folder. Returns the row count. None = nothing today: remove stale files."""
    path = storage.join(storage.join(gold_dir, table), f"dt={dt}")
    if df is None:
        storage.delete_prefix(path)
        return 0
    df = df.cache()
    n = df.count()
    df.write.mode("overwrite").parquet(path)
    df.unpersist()
    return n


def run(spark, dt, clean, gold_dir):
    configure(spark)  # session time zone = UTC: local_date() and the Parquet timestamps depend on it
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    t = {name: read_day(spark, clean, name, dt) for name in DAY_TABLES}
    out = {}

    if t["orders"] is not None:
        for need in ("order_items", "order_payments", "customer_snapshots"):
            if t[need] is None:
                raise SystemExit(f"{dt}: orders shipped but {need} did not")
        out["fact_orders"] = gold.build_fact_orders(
            t["orders"], t["order_items"], t["order_payments"], t["customer_snapshots"])
        out["fact_order_items"] = gold.build_fact_items(
            t["orders"], t["order_items"], t["customer_snapshots"])
        out["customer_feed"] = gold.build_customer_feed(t["customer_snapshots"], t["orders"])
    else:
        out["fact_orders"] = out["fact_order_items"] = out["customer_feed"] = None

    out["dim_product"] = (gold.build_dim_product(t["products"], t["product_category_name_translation"])
                          if t["products"] is not None and t["product_category_name_translation"] is not None
                          else None)
    out["dim_seller"] = gold.build_dim_seller(t["sellers"]) if t["sellers"] is not None else None

    events = read_events_upto(spark, clean, dt)
    out["fact_sessions"] = gold.build_sessions(events, dt) if events is not None else None

    print(f"gold for {dt}")
    for table, df in out.items():
        print(f"  {table:<18}{write_gold(df, gold_dir, table, dt):>9,} rows")
    # Items whose order is missing from the same day are dropped by the join. Make that visible.
    if t["order_items"] is not None and out["fact_order_items"] is not None:
        lost = t["order_items"].count() - out["fact_order_items"].count()
        if lost:
            print(f"  WARNING: {lost} order_items had no matching order on {dt} and were dropped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    ap.add_argument("--lake", default="data/local_lake")
    ap.add_argument("--clean", help="override <lake>/clean")
    ap.add_argument("--gold", help="override <lake>/gold")
    ap.add_argument("--env", default="local")
    a = ap.parse_args()
    clean = a.clean or storage.join(a.lake, "clean")
    gold_dir = a.gold or storage.join(a.lake, "gold")
    run(build_spark(a.env), a.dt, clean, gold_dir)


if __name__ == "__main__":
    main()