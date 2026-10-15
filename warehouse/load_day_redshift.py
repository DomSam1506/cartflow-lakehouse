"""Load one day of gold Parquet from S3 into Redshift Serverless via the Data API.

    python -m warehouse.load_day_redshift --dt 2017-11-24 --gold-bucket "$GOLD_BUCKET"
    python -m warehouse.load_day_redshift --all --gold-bucket "$GOLD_BUCKET"

One Data API BATCH = one transaction: DELETE staging, COPY each table, dimensions, facts.
If anything fails the batch rolls back. The invariant checks run AFTER (Redshift has no way to
fail a batch from a SELECT), so a failed check means: fix, then re-run the day (it is idempotent).
"""
import argparse
import calendar
from datetime import date, timedelta
from pathlib import Path

import boto3

from warehouse import rs_data

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql" / "redshift"

# gold folder -> staging table  (same mapping as the Postgres loader)
STAGING = {
    "dim_product": "stg_product",
    "dim_seller": "stg_seller",
    "customer_feed": "stg_customer",
    "fact_orders": "stg_orders",
    "fact_order_items": "stg_order_items",
    "fact_sessions": "stg_sessions",
}


def statements(sql_text):
    lines = [ln for ln in sql_text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def file_statements(name):
    return statements((SQL_DIR / name).read_text())


def dim_date_inserts(start=date(2016, 9, 1), end=date(2018, 12, 31), chunk=200):
    """dim_date as multi-row INSERTs (generate_series cannot feed a table on Redshift)."""
    rows, d = [], start
    while d <= end:
        dow = d.isoweekday()
        rows.append("({key}, '{iso}', {y}, {q}, {m}, '{mn}', {dd}, {dow}, '{dn}', {wk}, {bf})".format(
            key=int(d.strftime("%Y%m%d")), iso=d.isoformat(), y=d.year, q=(d.month - 1) // 3 + 1,
            m=d.month, mn=calendar.month_name[d.month], dd=d.day, dow=dow,
            dn=calendar.day_name[d.weekday()], wk="TRUE" if dow >= 6 else "FALSE",
            bf="TRUE" if d == date(2017, 11, 24) else "FALSE"))
        d += timedelta(days=1)
    head = "INSERT INTO dim_date VALUES "
    return [head + ", ".join(rows[i:i + chunk]) for i in range(0, len(rows), chunk)]


def setup():
    for stmt in file_statements("01_schema.sql") + file_statements("02_staging.sql"):
        rs_data.run(stmt)
    if int(rs_data.run("SELECT COUNT(*) FROM dim_date")[0][0]) == 0:
        for stmt in dim_date_inserts():
            rs_data.run(stmt)


def present_tables(s3, bucket, dt):
    """Which gold tables actually have files for this day (a day can have no sessions, etc.)."""
    out = []
    for table in STAGING:
        r = s3.list_objects_v2(Bucket=bucket, Prefix=f"{table}/dt={dt}/", MaxKeys=50)
        if any(o["Key"].endswith(".parquet") for o in r.get("Contents", [])):
            out.append(table)
    return out


def violations():
    rows = rs_data.run(Path(SQL_DIR / "90_checks.sql").read_text().rstrip().rstrip(";"))
    return [(name, int(n)) for name, n in rows if int(n) > 0]


def load_day(dt, bucket):
    s3 = boto3.client("s3")
    tables = present_tables(s3, bucket, dt)
    batch = [f"DELETE FROM {stg}" for stg in STAGING.values()]
    for t in tables:
        batch.append(f"COPY {STAGING[t]} FROM 's3://{bucket}/{t}/dt={dt}/' "
                     "IAM_ROLE DEFAULT FORMAT AS PARQUET")
    batch += file_statements("10_load_dimensions.sql") + file_statements("20_load_facts.sql")
    rs_data.run_batch(batch)               # one transaction, up to 40 statements
    bad = violations()
    if bad:
        raise SystemExit(f"{dt}: loaded, but invariant violations: {bad}")
    return tables


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dt")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--gold-bucket", required=True)
    a = ap.parse_args()
    setup()
    if a.all:
        s3 = boto3.client("s3")
        # Union over EVERY gold table: a day can have sessions (late events) but no orders.
        days = sorted({p["Prefix"].split("dt=")[1].strip("/")
                       for table in STAGING
                       for page in s3.get_paginator("list_objects_v2").paginate(
                           Bucket=a.gold_bucket, Prefix=f"{table}/", Delimiter="/")
                       for p in page.get("CommonPrefixes", [])
                       if "dt=" in p["Prefix"]})
    else:
        days = [a.dt]
    for dt in days:
        tables = load_day(dt, a.gold_bucket)
        print(f"loaded {dt}: {', '.join(tables)}")


if __name__ == "__main__":
    main()