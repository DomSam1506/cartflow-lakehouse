# scripts/profile_full.py
import duckdb

con = duckdb.connect()
D = "data/olist"
F = {
    "orders": f"{D}/olist_orders_dataset.csv",
    "items": f"{D}/olist_order_items_dataset.csv",
    "customers": f"{D}/olist_customers_dataset.csv",
    "products": f"{D}/olist_products_dataset.csv",
    "sellers": f"{D}/olist_sellers_dataset.csv",
    "payments": f"{D}/olist_order_payments_dataset.csv",
    "reviews": f"{D}/olist_order_reviews_dataset.csv",
    "geolocation": f"{D}/olist_geolocation_dataset.csv",
    "translation": f"{D}/product_category_name_translation.csv",
}


def show(title, sql):
    print(f"\n=== {title} ===")
    con.sql(sql).show()


# 1. TABLES AND ROW COUNTS (all 9, including the 2 not profiled yet)
for name, f in F.items():
    print(name, con.sql(f"select count(*) from '{f}'").fetchone()[0])

# 1b. GRAIN CHECKS (is the key really unique?)
show(
    "orders: order_id unique?",
    f"""
    select count(*) as rows, count(distinct order_id) as distinct_ids
    from '{F["orders"]}'""",
)
show(
    "customers: customer_id vs customer_unique_id",
    f"""
    select count(*) as rows,
           count(distinct customer_id) as distinct_customer_id,
           count(distinct customer_unique_id) as distinct_real_people
    from '{F["customers"]}'""",
)
show(
    "customers with more than one customer_id",
    f"""
    select count(*) as people_with_multiple_ids from (
        select customer_unique_id from '{F["customers"]}'
        group by 1 having count(*) > 1)""",
)
show(
    "order_items: items per order",
    f"""
    select count(distinct order_id) as orders, count(*) as items,
           round(count(*) * 1.0 / count(distinct order_id), 2) as avg_items_per_order,
           max(order_item_id) as max_items_in_one_order
    from '{F["items"]}'""",
)
show(
    "payments: payments per order",
    f"""
    select count(distinct order_id) as orders, count(*) as payments,
           max(payment_sequential) as max_payments_in_one_order
    from '{F["payments"]}'""",
)
show(
    "reviews: duplicate review_id",
    f"""
    select count(*) as rows, count(distinct review_id) as distinct_review_ids,
           count(distinct order_id) as distinct_orders
    from '{F["reviews"]}'""",
)
show(
    "reviews: orders with more than one review",
    f"""
    select count(*) as orders_with_multiple_reviews from (
        select order_id from '{F["reviews"]}' group by 1 having count(*) > 1)""",
)
show(
    "geolocation: rows per zip prefix",
    f"""
    select count(*) as rows,
           count(distinct geolocation_zip_code_prefix) as distinct_zips
    from '{F["geolocation"]}'""",
)

# 2. DATE RANGE + REPLAY WINDOW
show(
    "order date range",
    f"""
    select min(order_purchase_timestamp) as first_order,
           max(order_purchase_timestamp) as last_order
    from '{F["orders"]}'""",
)
show(
    "orders per month (find sparse ends)",
    f"""
    select strftime(order_purchase_timestamp, '%Y-%m') as month, count(*) as orders
    from '{F["orders"]}' group by 1 order by 1""",
)
show(
    "top 20 busiest days",
    f"""
    select date_trunc('day', order_purchase_timestamp)::date as day, count(*) as orders
    from '{F["orders"]}' group by 1 order by orders desc limit 20""",
)
show(
    "Nov 2017 daily volume (Black Friday candidate)",
    f"""
    select date_trunc('day', order_purchase_timestamp)::date as day, count(*) as orders
    from '{F["orders"]}'
    where order_purchase_timestamp >= '2017-11-10' and order_purchase_timestamp < '2017-12-10'
    group by 1 order by 1""",
)
show(
    "best 11-day window (rolling sum)",
    f"""
    with d as (
        select date_trunc('day', order_purchase_timestamp)::date as day, count(*) as n
        from '{F["orders"]}' group by 1)
    select day as window_start, day + 10 as window_end,
           sum(n) over (order by day rows between current row and 10 following) as orders_in_window
    from d order by orders_in_window desc limit 5""",
)

# 3. ORDER STATUS DISTRIBUTION (with percentages)
show(
    "order status",
    f"""
    select order_status, count(*) as orders,
           round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
    from '{F["orders"]}' group by 1 order by 2 desc""",
)

# 4. NULL RATES (per important column)
show(
    "orders null rates",
    f"""
    select count(*) as n,
      round(100.0 * count(*) filter (where order_approved_at is null) / count(*), 2) as approved_null_pct,
      round(100.0 * count(*) filter (where order_delivered_carrier_date is null) / count(*), 2) as carrier_null_pct,
      round(100.0 * count(*) filter (where order_delivered_customer_date is null) / count(*), 2) as delivered_null_pct,
      round(100.0 * count(*) filter (where order_estimated_delivery_date is null) / count(*), 2) as estimated_null_pct
    from '{F["orders"]}'""",
)
show(
    "delivered_date null by status (are nulls expected?)",
    f"""
    select order_status,
           count(*) as orders,
           count(*) filter (where order_delivered_customer_date is null) as null_delivered
    from '{F["orders"]}' group by 1 order by 2 desc""",
)
show(
    "delivered orders missing a delivery date (real defect)",
    f"""
    select count(*) as delivered_but_null_date
    from '{F["orders"]}'
    where order_status = 'delivered' and order_delivered_customer_date is null""",
)
show(
    "reviews null rates",
    f"""
    select count(*) as n,
      round(100.0 * count(*) filter (where review_comment_title is null) / count(*), 2) as title_null_pct,
      round(100.0 * count(*) filter (where review_comment_message is null) / count(*), 2) as message_null_pct,
      round(100.0 * count(*) filter (where review_score is null) / count(*), 2) as score_null_pct
    from '{F["reviews"]}'""",
)
show(
    "products null rates",
    f"""
    select count(*) as n,
      round(100.0 * count(*) filter (where product_category_name is null) / count(*), 2) as category_null_pct,
      round(100.0 * count(*) filter (where product_weight_g is null) / count(*), 2) as weight_null_pct,
      round(100.0 * count(*) filter (where product_photos_qty is null) / count(*), 2) as photos_null_pct
    from '{F["products"]}'""",
)

# 5. DATA QUALITY FINDINGS
show(
    "duplicate rows: fully identical rows per table",
    f"""
    select 'orders' as tbl, count(*) - count(distinct (order_id)) as dup_keys from '{F["orders"]}'
    union all
    select 'order_items', count(*) - count(distinct (order_id, order_item_id)) from '{F["items"]}'
    union all
    select 'payments', count(*) - count(distinct (order_id, payment_sequential)) from '{F["payments"]}'
    union all
    select 'products', count(*) - count(distinct product_id) from '{F["products"]}'
    union all
    select 'sellers', count(*) - count(distinct seller_id) from '{F["sellers"]}'
    union all
    select 'reviews (review_id)', count(*) - count(distinct review_id) from '{F["reviews"]}'""",
)
show(
    "impossible dates: delivered before purchase",
    f"""
    select count(*) as delivered_before_purchase
    from '{F["orders"]}'
    where order_delivered_customer_date < order_purchase_timestamp""",
)
show(
    "impossible dates: approved before purchase",
    f"""
    select count(*) as approved_before_purchase
    from '{F["orders"]}'
    where order_approved_at < order_purchase_timestamp""",
)
show(
    "impossible dates: carrier pickup before approval",
    f"""
    select count(*) as carrier_before_approval
    from '{F["orders"]}'
    where order_delivered_carrier_date < order_approved_at""",
)
show(
    "impossible dates: delivered to customer before carrier pickup",
    f"""
    select count(*) as customer_before_carrier
    from '{F["orders"]}'
    where order_delivered_customer_date < order_delivered_carrier_date""",
)
show(
    "late deliveries (delivered after estimate), delivered orders only",
    f"""
    select count(*) as delivered_orders,
           count(*) filter (where order_delivered_customer_date > order_estimated_delivery_date) as late,
           round(100.0 * count(*) filter (where order_delivered_customer_date > order_estimated_delivery_date) / count(*), 2) as late_pct
    from '{F["orders"]}' where order_status = 'delivered'""",
)
show(
    "payments: zero or negative values",
    f"""
    select count(*) filter (where payment_value = 0) as zero_value,
           count(*) filter (where payment_value < 0) as negative_value,
           count(*) filter (where payment_installments = 0) as zero_installments
    from '{F["payments"]}'""",
)
show(
    "payments: payment types",
    f"""
    select payment_type, count(*) as n from '{F["payments"]}' group by 1 order by 2 desc""",
)
show(
    "items: zero/negative price or freight",
    f"""
    select count(*) filter (where price <= 0) as bad_price,
           count(*) filter (where freight_value < 0) as negative_freight,
           min(price) as min_price, max(price) as max_price
    from '{F["items"]}'""",
)
show(
    "reviews: score distribution and out-of-range scores",
    f"""
    select review_score, count(*) as n from '{F["reviews"]}' group by 1 order by 1""",
)
show(
    "categories missing from the translation table",
    f"""
    select p.product_category_name, count(*) as products
    from '{F["products"]}' p
    left join '{F["translation"]}' t using (product_category_name)
    where t.product_category_name is null and p.product_category_name is not null
    group by 1 order by 2 desc""",
)
show(
    "orphans: items whose order is missing",
    f"""
    select count(*) as orphan_items
    from '{F["items"]}' i
    left join '{F["orders"]}' o using (order_id)
    where o.order_id is null""",
)
show(
    "orphans: orders with no items",
    f"""
    select count(*) as orders_without_items
    from '{F["orders"]}' o
    left join (select distinct order_id from '{F["items"]}') i using (order_id)
    where i.order_id is null""",
)
show(
    "orphans: orders with no payment",
    f"""
    select count(*) as orders_without_payment
    from '{F["orders"]}' o
    left join (select distinct order_id from '{F["payments"]}') p using (order_id)
    where p.order_id is null""",
)
show(
    "payment total vs item total mismatch (sample)",
    f"""
    with i as (select order_id, sum(price + freight_value) as item_total
               from '{F["items"]}' group by 1),
         p as (select order_id, sum(payment_value) as paid
               from '{F["payments"]}' group by 1)
    select count(*) as orders_compared,
           count(*) filter (where abs(i.item_total - p.paid) > 0.01) as mismatched
    from i join p using (order_id)""",
)

# 6. SCD2 MATERIAL (the customer ID trap)
show(
    "people whose zip/city/state changed across orders",
    f"""
    select count(*) as people_with_changing_address from (
        select customer_unique_id
        from '{F["customers"]}'
        group by 1
        having count(distinct (customer_zip_code_prefix, customer_city, customer_state)) > 1)""",
)
show(
    "customers per state (top 10)",
    f"""
    select customer_state, count(*) as customers
    from '{F["customers"]}' group by 1 order by 2 desc limit 10""",
)
show(
    "sellers per state (top 10)",
    f"""
    select seller_state, count(*) as sellers
    from '{F["sellers"]}' group by 1 order by 2 desc limit 10""",
)
