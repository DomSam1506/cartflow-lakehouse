"""SQL tests for the warehouse: MERGE loads, SCD Type 2, and the database's own guard rails.

Each test runs in a throw-away schema, so the data you loaded is never touched.
Needs the container:  docker compose up -d   (the tests skip themselves if it is not running)
"""
import uuid
from datetime import date

import psycopg
import pytest

from warehouse.load_day import DSN, apply_day, setup, violations

PRODUCT = ("p1", "toys", "toys", 10, 100, 1, 500, 10, 10, 10)
SELLER = ("s1", "01000", "sao paulo", "SP")


@pytest.fixture
def cur():
    try:
        admin = psycopg.connect(DSN, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("warehouse Postgres is not running (docker compose up -d)")
    schema = "t_" + uuid.uuid4().hex[:8]
    admin.execute(f"CREATE SCHEMA {schema}")
    conn = psycopg.connect(DSN, options=f"-c search_path={schema}")
    cursor = conn.cursor()
    setup(cursor)
    yield cursor
    conn.close()
    admin.execute(f"DROP SCHEMA {schema} CASCADE")
    admin.close()


def insert(cur, table, rows):
    for row in rows:
        marks = ",".join(["%s"] * len(row))
        cur.execute(f"INSERT INTO {table} VALUES ({marks})", row)


def load(cur, customers=(), orders=(), sessions=(), products=(PRODUCT,)):
    """Stage one day, run the real load SQL, and require every invariant check to pass.
    orders: (order_id, person, day). Each order gets one item priced 10 with 2 freight."""
    cur.execute("TRUNCATE stg_customer, stg_product, stg_seller, stg_orders, stg_order_items, stg_sessions")
    insert(cur, "stg_product", products)
    insert(cur, "stg_seller", [SELLER])
    insert(cur, "stg_customer", customers)
    insert(cur, "stg_orders", [
        (oid, uid, day, "delivered", f"{day} 12:00", f"{day} 13:00", None, None, None, None, 1, 10, 2, 12)
        for oid, uid, day in orders])
    insert(cur, "stg_order_items", [(oid, 1, "p1", "s1", uid, day, 10, 2) for oid, uid, day in orders])
    insert(cur, "stg_sessions", sessions)
    apply_day(cur)
    assert violations(cur) == []


def scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def counts(cur):
    cur.execute("SELECT (SELECT count(*) FROM fact_orders), (SELECT count(*) FROM fact_order_items),"
                " (SELECT count(*) FROM dim_customer)")
    return cur.fetchone()


DAY1 = dict(customers=[("u1", "01046", "sao paulo", "SP", "2017-11-20")], orders=[("o1", "u1", "2017-11-20")])
DAY2 = dict(customers=[("u1", "20000", "rio", "RJ", "2017-11-22")], orders=[("o2", "u1", "2017-11-22")])


def order_addresses(cur):
    cur.execute("SELECT o.order_id, c.zip FROM fact_orders o JOIN dim_customer c USING (customer_sk) ORDER BY 1")
    return cur.fetchall()


def test_loading_a_day_twice_creates_no_duplicates(cur):
    load(cur, **DAY1)
    load(cur, **DAY1)
    assert counts(cur) == (1, 1, 1)


def test_address_change_closes_the_old_row_and_opens_a_new_one(cur):
    load(cur, **DAY1)
    load(cur, **DAY2)
    cur.execute("SELECT zip, valid_from, valid_to, is_current FROM dim_customer ORDER BY valid_from")
    assert cur.fetchall() == [
        ("01046", date(2017, 11, 20), date(2017, 11, 22), False),
        ("20000", date(2017, 11, 22), None, True),
    ]


def test_each_order_keeps_the_address_it_was_placed_with(cur):
    load(cur, **DAY1)
    load(cur, **DAY2)
    assert order_addresses(cur) == [("o1", "01046"), ("o2", "20000")]


def test_replaying_an_old_day_keeps_history_intact(cur):
    load(cur, **DAY1)
    load(cur, **DAY2)
    load(cur, **DAY1)  # the customer has moved since, but day 1 must still resolve to the old address
    assert scalar(cur, "SELECT count(*) FROM dim_customer") == 2
    assert order_addresses(cur) == [("o1", "01046"), ("o2", "20000")]


def test_a_stale_snapshot_cannot_rewrite_history(cur):
    load(cur, **DAY1)
    load(cur, **DAY2)
    load(cur, customers=[("u1", "99999", "manaus", "AM", "2017-11-21")])  # older than the current row
    cur.execute("SELECT zip, is_current FROM dim_customer ORDER BY valid_from")
    assert cur.fetchall() == [("01046", False), ("20000", True)]


def test_the_checks_catch_overlapping_validity_ranges(cur):
    load(cur, **DAY1)
    load(cur, **DAY2)
    cur.execute("INSERT INTO dim_customer (customer_unique_id, zip, city, state, valid_from, valid_to, is_current)"
                " VALUES ('u1', '99999', 'x', 'XX', '2017-11-21', '2017-11-23', FALSE)")
    assert "overlapping validity ranges" in dict(violations(cur))


def test_the_database_refuses_a_second_current_row(cur):
    load(cur, **DAY1)
    with pytest.raises(psycopg.errors.UniqueViolation):
        cur.execute("INSERT INTO dim_customer (customer_unique_id, zip, city, state, valid_from, valid_to, is_current)"
                    " VALUES ('u1', '02000', 'y', 'RJ', '2017-11-22', NULL, TRUE)")


def test_the_database_refuses_an_end_date_before_the_start_date(cur):
    with pytest.raises(psycopg.errors.CheckViolation):
        cur.execute("INSERT INTO dim_customer (customer_unique_id, zip, city, state, valid_from, valid_to, is_current)"
                    " VALUES ('u9', '01000', 'x', 'SP', '2017-11-20', '2017-11-19', FALSE)")


def test_a_changed_product_is_overwritten_in_place(cur):
    load(cur)
    load(cur, products=[("p1", "toys", "toys", 10, 100, 1, 999, 10, 10, 10)])
    cur.execute("SELECT count(*), max(weight_g) FROM dim_product")
    assert cur.fetchone() == (1, 999)


def test_a_session_is_replaced_not_duplicated(cur):
    start = ("s-1", "u1", "2017-11-20", "mobile", "2017-11-20 10:00")
    load(cur, sessions=[(*start, "2017-11-20 10:05", 2, "add_to_cart", True, True, False, False)])
    load(cur, sessions=[(*start, "2017-11-20 10:09", 4, "purchase", True, True, True, True)])
    cur.execute("SELECT count(*), max(n_events), bool_and(purchased) FROM fact_sessions")
    assert cur.fetchone() == (1, 4, True)


def test_duplicate_staging_rows_fail_loudly(cur):
    row = ("u1", "01046", "sao paulo", "SP", "2017-11-20")
    insert(cur, "stg_customer", [row])
    with pytest.raises(psycopg.errors.UniqueViolation):
        insert(cur, "stg_customer", [row])