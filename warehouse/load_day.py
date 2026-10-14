"""Load one day of gold Parquet into the Postgres warehouse, all in ONE transaction.

    python -m warehouse.load_day --dt 2017-11-24

Steps: truncate staging -> COPY the day's gold files in -> dimensions -> facts -> invariant checks.
If any check finds a violation the whole day rolls back, so the warehouse never holds a bad day.
"""
import argparse
import os
from pathlib import Path

import duckdb
import psycopg

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql" / "postgres"
DSN = os.environ.get("WAREHOUSE_DSN", "postgresql://cartflow:cartflow@localhost:5433/cartflow")

# gold folder -> staging table
STAGING = {
    "dim_product": "stg_product",
    "dim_seller": "stg_seller",
    "customer_feed": "stg_customer",
    "fact_orders": "stg_orders",
    "fact_order_items": "stg_order_items",
    "fact_sessions": "stg_sessions",
}


def statements(sql_text):
    """Split a .sql file into single statements. Full-line comments are dropped first."""
    lines = [ln for ln in sql_text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def run_file(cur, name):
    for stmt in statements((SQL_DIR / name).read_text()):
        cur.execute(stmt)


def setup(cur):
    run_file(cur, "01_schema.sql")
    run_file(cur, "02_staging.sql")


def apply_day(cur):
    """Dimensions first (facts look up customer_sk), then facts. Staging must already be filled."""
    run_file(cur, "10_load_dimensions.sql")
    run_file(cur, "20_load_facts.sql")


def violations(cur):
    cur.execute((SQL_DIR / "90_checks.sql").read_text().rstrip().rstrip(";"))
    return [(name, n) for name, n in cur.fetchall() if n > 0]


def stage(cur, gold_dir, dt):
    cur.execute("TRUNCATE " + ", ".join(STAGING.values()))
    con = duckdb.connect()
    # Spark writes timestamps as UTC instants. Pin DuckDB to UTC and drop the zone, so Postgres
    # receives the same wall-clock UTC value that is in the file (not one shifted to this machine's zone).
    con.execute("SET TimeZone = 'UTC'")
    loaded = {}
    for table, stg in STAGING.items():
        folder = Path(gold_dir) / table / f"dt={dt}"
        if not list(folder.glob("*.parquet")):
            loaded[table] = 0
            continue
        # hive_partitioning=false: otherwise DuckDB adds a `dt` column taken from the folder name
        src = f"read_parquet('{folder}/*.parquet', hive_partitioning=false)"
        probe = con.sql(f"SELECT * FROM {src}")
        cols = probe.columns
        select = ", ".join(
            f'CAST("{c}" AS TIMESTAMP) AS "{c}"' if "TIME ZONE" in str(t) else f'"{c}"'
            for c, t in zip(cols, probe.types))
        rows = con.sql(f"SELECT {select} FROM {src}").fetchall()
        with cur.copy(f"COPY {stg} ({', '.join(cols)}) FROM STDIN") as cp:
            for row in rows:
                cp.write_row(row)
        loaded[table] = len(rows)
    return loaded


def load_day(dt, gold_dir="data/local_lake/gold", dsn=DSN):
    with psycopg.connect(dsn) as conn:  # commits on success, rolls back on any exception
        with conn.cursor() as cur:
            setup(cur)
            loaded = stage(cur, gold_dir, dt)
            apply_day(cur)
            bad = violations(cur)
            if bad:
                raise SystemExit(f"{dt}: invariant violations, rolled back: {bad}")
    return loaded


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    ap.add_argument("--gold", default="data/local_lake/gold")
    a = ap.parse_args()
    loaded = load_day(a.dt, a.gold)
    print(f"loaded {a.dt}: " + ", ".join(f"{k}={v:,}" for k, v in loaded.items()))


if __name__ == "__main__":
    main()