"""Which purchased sessions differ between Postgres and Redshift, and how.

    PYTHONPATH=. python scripts/diff_sessions.py
"""
import os

import psycopg

from warehouse import rs_data

DSN = os.environ.get("WAREHOUSE_DSN", "postgresql://cartflow:cartflow@localhost:5433/cartflow")
COLS = "session_id, session_date, n_events, session_end, furthest_stage, purchased"

with psycopg.connect(DSN) as conn, conn.cursor() as cur:
    cur.execute("SELECT session_id FROM fact_sessions WHERE purchased")
    pg = {r[0] for r in cur.fetchall()}
    rs = {r[0] for r in rs_data.run("SELECT session_id FROM fact_sessions WHERE purchased")}
    only_pg, only_rs = sorted(pg - rs), sorted(rs - pg)
    print(f"purchased: postgres {len(pg)}, redshift {len(rs)}; only in postgres {len(only_pg)}, only in redshift {len(only_rs)}")
    if not only_pg and not only_rs:
        raise SystemExit("no difference")
    ids = ",".join("'" + i + "'" for i in (only_pg + only_rs)[:300])
    cur.execute(f"SELECT {COLS} FROM fact_sessions WHERE session_id IN ({ids}) ORDER BY session_date, session_id")
    pg_rows = {r[0]: r for r in cur.fetchall()}
    rs_rows = {r[0]: r for r in rs_data.run(f"SELECT {COLS} FROM fact_sessions WHERE session_id IN ({ids})")}
    by_day = {}
    for sid, p in pg_rows.items():
        by_day[str(p[1])] = by_day.get(str(p[1]), 0) + 1
    print("differing sessions by session_date:", dict(sorted(by_day.items())))
    print("\nfirst 6 examples (postgres vs redshift): n_events, session_end, furthest_stage, purchased")
    for sid in list(pg_rows)[:6]:
        p, r = pg_rows[sid], rs_rows.get(sid)
        print(sid[:10], "| PG:", p[2], str(p[3]), p[4], p[5], "| RS:", None if r is None else (r[2], r[3], r[4], r[5]))