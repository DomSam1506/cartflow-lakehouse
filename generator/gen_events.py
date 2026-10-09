# generator/gen_events.py
"""Seeded clickstream generator for CartFlow.

One converting session per real Olist order (its purchase event lands exactly on
order_purchase_timestamp), plus many non-converting browse sessions per day.
Defects are injected AFTER generation so the quality gates have something to catch.

    python generator/gen_events.py --start 2017-11-20 --end 2017-11-30
"""
import argparse
import gzip
import json
import uuid
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

FUNNEL = ["page_view", "add_to_cart", "checkout_start", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_P = [0.60, 0.35, 0.05]
# relative traffic by hour of day (night low, evening peak)
HOUR_W = np.array([1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 7,
                   7, 7, 7, 7, 7, 8, 8, 9, 9, 8, 5, 2], dtype=float)
HOUR_P = HOUR_W / HOUR_W.sum()


def new_id(rng):
    """Seeded UUID4 (uuid.uuid4() is not seedable, which would break reproducibility)."""
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


def session_events(rng, sid, cust, product, device, stages, end_ts=None, start_ts=None):
    """Yield the events of one session.
    Converting sessions are built backwards from end_ts so 'purchase' == order timestamp.
    Browse sessions are built forwards from start_ts."""
    gaps = [pd.Timedelta(seconds=int(g)) for g in rng.integers(5, 240, size=stages - 1)]
    times = [None] * stages
    if end_ts is not None:
        times[-1] = end_ts
        for i in range(stages - 2, -1, -1):
            times[i] = times[i + 1] - gaps[i]
    else:
        times[0] = start_ts
        for i in range(1, stages):
            times[i] = times[i - 1] + gaps[i - 1]
    for stage, ts in zip(FUNNEL[:stages], times):
        yield {
            "event_id": new_id(rng),
            "session_id": sid,
            "customer_unique_id": cust,
            "product_id": product,
            "event_type": stage,
            "event_ts": ts.isoformat(),
            "device": device,
        }


def load_inputs(src, start, end):
    con = duckdb.connect()
    orders = con.sql(f"""
        select o.order_id, c.customer_unique_id, o.order_purchase_timestamp as purchase_ts,
               i.product_id
        from '{src}/olist_orders_dataset.csv' o
        join '{src}/olist_customers_dataset.csv' c using (customer_id)
        left join '{src}/olist_order_items_dataset.csv' i
               on i.order_id = o.order_id and i.order_item_id = 1
        where o.order_purchase_timestamp >= '{start}'
          and o.order_purchase_timestamp <  date '{end}' + interval 1 day
        order by o.order_purchase_timestamp""").df()
    customers = con.sql(
        f"select distinct customer_unique_id from '{src}/olist_customers_dataset.csv'"
    ).df()["customer_unique_id"].to_numpy()
    products = con.sql(
        f"select distinct product_id from '{src}/olist_order_items_dataset.csv'"
    ).df()["product_id"].to_numpy()
    return orders, customers, products


def generate(orders, customers, products, browse_ratio, rng):
    events = []
    sessions = 0

    # 1) one converting session per real order
    for r in orders.itertuples(index=False):
        product = r.product_id if isinstance(r.product_id, str) else rng.choice(products)
        device = rng.choice(DEVICES, p=DEVICE_P)
        events.extend(session_events(rng, new_id(rng), r.customer_unique_id, product,
                                     device, 4, end_ts=pd.Timestamp(r.purchase_ts)))
        sessions += 1

    # 2) per day: browse_ratio x as many non-converting sessions
    days = orders["purchase_ts"].dt.normalize()
    for day, n_orders in days.value_counts().sort_index().items():
        n = int(n_orders * browse_ratio)
        hours = rng.choice(24, size=n, p=HOUR_P)
        for h in hours:
            start = day + pd.Timedelta(hours=int(h), seconds=int(rng.integers(0, 3600)))
            stages = int(rng.choice([1, 2, 3], p=[0.70, 0.22, 0.08]))
            events.extend(session_events(
                rng, new_id(rng), rng.choice(customers), rng.choice(products),
                rng.choice(DEVICES, p=DEVICE_P), stages, start_ts=start))
            sessions += 1
    return pd.DataFrame(events), sessions


def inject_defects(df, rng):
    """Returns (df_with_defects, counts). file_day decides which day's file a row lands in."""
    df = df.copy()
    df["file_day"] = pd.to_datetime(df["event_ts"]).dt.normalize()
    n = len(df)
    counts = {}

    # ~2% late arrivals: written to the NEXT day's file
    late = rng.choice(n, int(0.02 * n), replace=False)
    df.loc[df.index[late], "file_day"] += pd.Timedelta(days=1)
    counts["late_arrivals"] = len(late)

    # ~0.5% malformed rows
    bad = rng.choice(n, int(0.005 * n), replace=False)
    df["event_ts"] = df["event_ts"].astype(object)
    df["event_id"] = df["event_id"].astype(object)
    kinds = rng.integers(0, 4, size=len(bad))
    for i, k in zip(df.index[bad], kinds):
        if k == 0:
            df.at[i, "event_id"] = None
        elif k == 1:
            df.at[i, "event_type"] = "page_viw"
        elif k == 2:
            df.at[i, "event_ts"] = "not-a-timestamp"
        else:
            df.at[i, "event_ts"] = "1970-01-01T00:00:00"
    counts["malformed"] = len(bad)

    # ~1% exact duplicates (same event_id, same file)
    dup = rng.choice(n, int(0.01 * n), replace=False)
    df = pd.concat([df, df.iloc[dup]], ignore_index=True)
    counts["duplicates"] = len(dup)

    return df.sample(frac=1, random_state=42).reset_index(drop=True), counts


def write_partitions(df, out):
    out = Path(out)
    for day, g in df.groupby("file_day"):
        d = out / f"dt={day.date()}"
        d.mkdir(parents=True, exist_ok=True)
        rows = g.drop(columns="file_day").to_dict("records")
        with open(d / "part-0.json.gz", "wb") as raw, \
                gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:  # mtime=0: deterministic bytes
            for row in rows:
                gz.write((json.dumps(row) + "\n").encode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/olist")
    ap.add_argument("--out", default="data/generated/events")
    ap.add_argument("--start", default="2017-11-20")
    ap.add_argument("--end", default="2017-11-30")
    ap.add_argument("--browse-ratio", type=float, default=30.0,
                    help="non-converting sessions per real order")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    rng = np.random.default_rng(a.seed)
    orders, customers, products = load_inputs(a.src, a.start, a.end)
    clean, sessions = generate(orders, customers, products, a.browse_ratio, rng)
    dirty, counts = inject_defects(clean, rng)
    write_partitions(dirty, a.out)

    # ---- sanity check ----
    print(f"\nwindow {a.start}..{a.end}: {len(orders):,} real orders, {sessions:,} sessions")
    print("\nfunnel (clean events):")
    f = clean["event_type"].value_counts().reindex(FUNNEL)
    for stage, c in f.items():
        print(f"  {stage:<15}{c:>10,}  {c / f.iloc[0]:6.1%} of page_views")
    print(f"\npurchase conversion (sessions): {len(orders) / sessions:.2%}")
    print(f"clean events: {len(clean):,}   after defects: {len(dirty):,}")
    print("injected:", counts)
    print(f"files written under {a.out}: {dirty['file_day'].nunique()} day partitions")


if __name__ == "__main__":
    main()