"""Compare the local Postgres warehouse with Redshift: row counts and money totals must match.

    python scripts/reconcile_redshift.py
"""
import os
from decimal import Decimal

import psycopg
import sys; from pathlib import Path; sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from warehouse import rs_data

DSN = os.environ.get("WAREHOUSE_DSN", "postgresql://cartflow:cartflow@localhost:5433/cartflow")

CHECKS = [
    ("dim_date rows", "SELECT COUNT(*) FROM dim_date"),
    ("dim_customer rows", "SELECT COUNT(*) FROM dim_customer"),
    ("dim_customer current rows", "SELECT COUNT(*) FROM dim_customer WHERE is_current"),
    ("dim_product rows", "SELECT COUNT(*) FROM dim_product"),
    ("dim_seller rows", "SELECT COUNT(*) FROM dim_seller"),
    ("fact_orders rows", "SELECT COUNT(*) FROM fact_orders"),
    ("fact_order_items rows", "SELECT COUNT(*) FROM fact_order_items"),
    ("fact_sessions rows", "SELECT COUNT(*) FROM fact_sessions"),
    ("sessions purchased", "SELECT COUNT(*) FROM fact_sessions WHERE purchased"),
    ("orders delivered_late", "SELECT COUNT(*) FROM fact_orders WHERE delivered_late"),
    ("sum items_value", "SELECT SUM(items_value) FROM fact_orders"),
    ("sum freight_value", "SELECT SUM(freight_value) FROM fact_orders"),
    ("sum payment_value", "SELECT SUM(payment_value) FROM fact_orders"),
    ("sum item price", "SELECT SUM(price) FROM fact_order_items"),
]


def main():
    bad = 0
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        print(f"{'check':<28}{'postgres':>16}{'redshift':>16}")
        for name, sql in CHECKS:
            cur.execute(sql)
            pg = Decimal(str(cur.fetchone()[0]))
            rs = Decimal(str(rs_data.run(sql)[0][0]))
            ok = pg == rs
            bad += not ok
            print(f"{name:<28}{pg:>16,}{rs:>16,}  {'ok' if ok else 'MISMATCH'}")
    print("\nALL MATCH" if not bad else f"\n{bad} MISMATCH(ES)")
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()