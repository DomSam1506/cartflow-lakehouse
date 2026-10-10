# generator/slice_tables.py
"""Slice the Olist tables into one folder per day (the staging area replay.py ships from).

    python generator/slice_tables.py --start 2017-11-20 --end 2017-11-30

Output: data/generated/tables/dt=YYYY-MM-DD/<table>/part-0.csv.gz
  orders, order_items, order_payments   sliced by the order's purchase date
  order_reviews                         sliced by review_creation_date (orders in the window only)
  customer_snapshots                    one row per customer who ordered that day (SCD2 feed)
  products, sellers, product_category_name_translation   static: first day only
"""

import argparse
import gzip
from pathlib import Path

import pandas as pd

STATIC = {
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


def write_csv_gz(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
        gz.write(df.to_csv(index=False).encode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/olist")
    ap.add_argument("--out", default="data/generated/tables")
    ap.add_argument("--start", default="2017-11-20")
    ap.add_argument("--end", default="2017-11-30")
    a = ap.parse_args()
    src, out = Path(a.src), Path(a.out)

    orders = pd.read_csv(src / "olist_orders_dataset.csv")
    orders["day"] = pd.to_datetime(orders["order_purchase_timestamp"]).dt.strftime(
        "%Y-%m-%d"
    )
    orders = orders[(orders["day"] >= a.start) & (orders["day"] <= a.end)]
    order_day = orders.set_index("order_id")["day"]

    items = pd.read_csv(src / "olist_order_items_dataset.csv")
    items["day"] = items["order_id"].map(order_day)
    items = items.dropna(subset=["day"])

    pays = pd.read_csv(src / "olist_order_payments_dataset.csv")
    pays["day"] = pays["order_id"].map(order_day)
    pays = pays.dropna(subset=["day"])

    reviews = pd.read_csv(src / "olist_order_reviews_dataset.csv")
    reviews["day"] = pd.to_datetime(reviews["review_creation_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    reviews = reviews[
        reviews["order_id"].isin(order_day.index)
        & (reviews["day"] >= a.start)
        & (reviews["day"] <= a.end)
    ]

    cust = pd.read_csv(src / "olist_customers_dataset.csv")
    snap = (
        orders[["order_id", "customer_id", "order_purchase_timestamp", "day"]]
        .merge(cust, on="customer_id")
        .sort_values("order_purchase_timestamp")
        .drop_duplicates(["customer_unique_id", "day"], keep="last")
        .rename(
            columns={
                "customer_zip_code_prefix": "zip",
                "customer_city": "city",
                "customer_state": "state",
                "day": "effective_date",
            }
        )[["customer_unique_id", "zip", "city", "state", "effective_date"]]
    )
    snap["day"] = snap["effective_date"]

    days = sorted(pd.date_range(a.start, a.end).strftime("%Y-%m-%d"))
    tables = {
        "orders": orders,
        "order_items": items,
        "order_payments": pays,
        "order_reviews": reviews,
        "customer_snapshots": snap,
    }
    for day in days:
        for name, df in tables.items():
            part = df[df["day"] == day].drop(columns="day")
            if len(part):
                write_csv_gz(part, out / f"dt={day}" / name / "part-0.csv.gz")
    for name, fname in STATIC.items():
        write_csv_gz(
            pd.read_csv(src / fname), out / f"dt={days[0]}" / name / "part-0.csv.gz"
        )

    print(f"window {a.start}..{a.end}")
    for name, df in tables.items():
        print(f"  {name:<20}{len(df):>9,} rows")
    print(f"static tables written to dt={days[0]} only: {', '.join(STATIC)}")


if __name__ == "__main__":
    main()
