"""Three-way reconciliation: every source row must be either in the warehouse or in quarantine.

    python scripts/reconcile.py

PASS means: source (minus orders rejected by data quality) == warehouse. Rejected orders are listed.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import psycopg

from warehouse.load_day import DSN

START, END = "2017-11-20", "2017-11-30"
SRC = "data/olist"
LAKE = Path("data/local_lake")


def main():
    d = duckdb.connect()
    pg = psycopg.connect(DSN)

    def src(sql):
        return d.sql(sql).fetchone()[0]

    def wh(sql):
        with pg.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]

    orders_csv = f"read_csv_auto('{SRC}/olist_orders_dataset.csv')"
    items_csv = f"read_csv_auto('{SRC}/olist_order_items_dataset.csv')"
    pays_csv = f"read_csv_auto('{SRC}/olist_order_payments_dataset.csv')"
    cust_csv = f"read_csv_auto('{SRC}/olist_customers_dataset.csv')"
    in_window = (f"(SELECT order_id FROM {orders_csv} "
                 f"WHERE CAST(order_purchase_timestamp AS DATE) BETWEEN '{START}' AND '{END}')")

    quarantine = LAKE / "quarantine" / "orders"
    has_quarantine = bool(list(quarantine.glob("*/*.parquet")))
    quarantine_sql = f"read_parquet('{quarantine}/*/*.parquet')"
    if has_quarantine:
        accepted = (f"(SELECT order_id FROM {in_window} "
                    f"WHERE order_id NOT IN (SELECT DISTINCT order_id FROM {quarantine_sql}))")
    else:
        accepted = in_window

    metrics = [
        ("orders", lambda w: f"SELECT count(*) FROM {orders_csv} WHERE order_id IN {w}",
         "SELECT count(*) FROM fact_orders"),
        ("order items", lambda w: f"SELECT count(*) FROM {items_csv} WHERE order_id IN {w}",
         "SELECT count(*) FROM fact_order_items"),
        ("items value", lambda w: f"SELECT sum(price) FROM {items_csv} WHERE order_id IN {w}",
         "SELECT sum(items_value) FROM fact_orders"),
        ("freight value", lambda w: f"SELECT sum(freight_value) FROM {items_csv} WHERE order_id IN {w}",
         "SELECT sum(freight_value) FROM fact_orders"),
        ("payments", lambda w: f"SELECT sum(payment_value) FROM {pays_csv} WHERE order_id IN {w}",
         "SELECT sum(payment_value) FROM fact_orders"),
        ("people who ordered",
         lambda w: (f"SELECT count(DISTINCT c.customer_unique_id) FROM {orders_csv} o "
                    f"JOIN {cust_csv} c USING (customer_id) WHERE o.order_id IN {w}"),
         "SELECT count(DISTINCT customer_unique_id) FROM dim_customer"),
    ]
    rows = [(name, src(q(in_window)), src(q(accepted)), wh(w)) for name, q, w in metrics]

    clean_sessions = src(f"SELECT count(DISTINCT session_id) FROM read_parquet('{LAKE}/clean/events/*/*.parquet')")
    products = src(f"SELECT count(*) FROM read_csv_auto('{SRC}/olist_products_dataset.csv')")
    sellers = src(f"SELECT count(*) FROM read_csv_auto('{SRC}/olist_sellers_dataset.csv')")
    rows += [
        ("sessions (vs clean events)", clean_sessions, clean_sessions, wh("SELECT count(*) FROM fact_sessions")),
        ("products", products, products, wh("SELECT count(*) FROM dim_product")),
        ("sellers", sellers, sellers, wh("SELECT count(*) FROM dim_seller")),
    ]

    failed = 0
    print(f"{'check':<28}{'source':>14}{'rejected':>12}{'warehouse':>14}  result")
    for name, everything, kept, got in rows:
        ok = abs(float(kept) - float(got)) < 0.005
        failed += not ok
        print(f"{name:<28}{float(everything):>14,.2f}{float(everything) - float(kept):>12,.2f}"
              f"{float(got):>14,.2f}  {'PASS' if ok else 'FAIL'}")

    if has_quarantine:
        rejected = d.sql(f"SELECT order_id, order_status, dq_reason FROM {quarantine_sql} "
                         f"WHERE order_id IN {in_window}").fetchall()
        print("\norders rejected by data quality (kept in quarantine, not in the warehouse):")
        for r in rejected:
            print("  ", r)

    orders = wh("SELECT count(*) FROM fact_orders")
    sessions = wh("SELECT count(*) FROM fact_sessions")
    purchased = wh("SELECT count(*) FROM fact_sessions WHERE purchased")
    moved = wh("SELECT count(*) FROM (SELECT 1 FROM dim_customer GROUP BY customer_unique_id HAVING count(*) > 1) x")
    print()
    print(f"info: purchased sessions {purchased:,} vs orders {orders:,} ({purchased / orders:.1%}); "
          "the small gap is purchase events lost to quarantine")
    print(f"info: conversion {purchased / sessions:.2%} of {sessions:,} sessions")
    print(f"info: people with more than one address version in the real data: {moved}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()