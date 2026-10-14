"""Show SCD Type 2 on a REAL customer using a FABRICATED move. Everything is rolled back at the end.

    python scripts/demo_scd2.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg

from warehouse.load_day import DSN, STAGING, apply_day, setup


def show(cur, uid, title):
    cur.execute("SELECT customer_sk, zip, city, state, valid_from, valid_to, is_current "
                "FROM dim_customer WHERE customer_unique_id = %s ORDER BY valid_from", (uid,))
    print(title)
    for row in cur.fetchall():
        print("  ", row)


with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        setup(cur)
        cur.execute("SELECT customer_unique_id FROM dim_customer WHERE is_current "
                    "ORDER BY customer_unique_id LIMIT 1")
        uid = cur.fetchone()[0]
        show(cur, uid, "before:")
        cur.execute("TRUNCATE " + ", ".join(STAGING.values()))
        cur.execute("INSERT INTO stg_customer VALUES (%s, '90000', 'fabricated city', 'RS', '2017-12-05')", (uid,))
        apply_day(cur)
        show(cur, uid, "after a FABRICATED move on 2017-12-05:")
        cur.execute("SELECT o.order_id, c.zip FROM fact_orders o JOIN dim_customer c USING (customer_sk) "
                    "WHERE o.customer_unique_id = %s", (uid,))
        print("their real orders still point at:", cur.fetchall())
        conn.rollback()
        print("rolled back: the warehouse is unchanged")