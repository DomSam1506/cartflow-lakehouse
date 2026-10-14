"""Pure DataFrame -> DataFrame builders for the gold layer. No file I/O, so they are easy to test."""
from pyspark.sql import Window
from pyspark.sql import functions as F

LOCAL_TZ = "America/Sao_Paulo"
STAGES = ["page_view", "add_to_cart", "checkout_start", "purchase"]


def local_date(col_name):
    """UTC timestamp column -> Brazil calendar date. This is the business day we report on."""
    return F.to_date(F.from_utc_timestamp(F.col(col_name), LOCAL_TZ))


def dedupe_events(events):
    """Same event_id shipped on two days (a late duplicate): keep the earliest arrival."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_dt").asc(), F.col("event_ts").asc())
    return events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_sessions(events, dt):
    """One row per session touched by day `dt`, recomputed from ALL events up to `dt`.

    A late event can complete a session that started yesterday. Recomputing from history
    means every row we emit is complete, so the warehouse can replace it wholesale.
    """
    ev = dedupe_events(events.filter(F.col("ingest_dt") <= dt))
    touched = ev.filter(F.col("ingest_dt") == dt).select("session_id").distinct()
    ev = ev.join(touched, "session_id", "left_semi")

    rank = F.create_map(*[x for i, s in enumerate(STAGES, 1) for x in (F.lit(s), F.lit(i))])
    name = F.create_map(*[x for i, s in enumerate(STAGES, 1) for x in (F.lit(i), F.lit(s))])
    ev = ev.withColumn("stage_rank", F.element_at(rank, F.col("event_type")))

    s = ev.groupBy("session_id").agg(
        F.max("customer_unique_id").alias("customer_unique_id"),
        F.max("device").alias("device"),
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").cast("int").alias("n_events"),
        F.max("stage_rank").alias("max_rank"),
    )
    # "Reached" semantics: a session that purchased also counts as carted and checked out,
    # even if one of those events was quarantined. This keeps the funnel monotone.
    return s.select(
        "session_id", "customer_unique_id",
        local_date("session_start").alias("session_date"),
        "device", "session_start", "session_end", "n_events",
        F.element_at(name, F.col("max_rank")).alias("furthest_stage"),
        (F.col("max_rank") >= 1).alias("viewed"),
        (F.col("max_rank") >= 2).alias("carted"),
        (F.col("max_rank") >= 3).alias("checkout_started"),
        (F.col("max_rank") >= 4).alias("purchased"),
    )


def attach_customer(orders, snapshots):
    """orders.customer_id -> customer_unique_id. Fails loudly if any order cannot be linked."""
    m = snapshots.select("customer_id", "customer_unique_id").dropDuplicates(["customer_id"])
    out = orders.join(m, "customer_id", "left")
    missing = out.filter(F.col("customer_unique_id").isNull()).count()
    if missing:
        raise ValueError(f"{missing} orders have no customer snapshot; re-slice and re-clean customer_snapshots")
    return out


def build_customer_feed(snapshots, orders):
    """One address per person per day (their latest order that day), ready for the SCD2 load."""
    o = orders.select("customer_id", F.col("order_purchase_timestamp").alias("purchase_ts"))
    s = snapshots.join(o, "customer_id")
    w = Window.partitionBy("customer_unique_id", "effective_date").orderBy(
        F.col("purchase_ts").desc(), F.col("customer_id"))
    return (s.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .select("customer_unique_id",
                    F.lpad(F.col("zip").cast("string"), 5, "0").alias("zip"),  # CSV lost leading zeros
                    "city", "state",
                    F.to_date("effective_date").alias("effective_date")))


def build_fact_orders(orders, items, payments, snapshots):
    o = attach_customer(orders, snapshots)
    ia = items.groupBy("order_id").agg(
        F.count("*").cast("int").alias("n_items"),
        F.sum("price").alias("items_value"),
        F.sum("freight_value").alias("freight_value"))
    pa = payments.groupBy("order_id").agg(F.sum("payment_value").alias("payment_value"))
    delivered = F.col("order_delivered_customer_date")
    estimated = F.col("order_estimated_delivery_date")
    zero = F.lit(0)
    return (o.join(ia, "order_id", "left").join(pa, "order_id", "left").select(
        "order_id", "customer_unique_id",
        local_date("order_purchase_timestamp").alias("order_date"),
        "order_status",
        F.col("order_purchase_timestamp").alias("purchase_ts"),
        F.col("order_approved_at").alias("approved_ts"),
        F.col("order_delivered_carrier_date").alias("delivered_carrier_ts"),
        delivered.alias("delivered_customer_ts"),
        estimated.alias("estimated_delivery_ts"),
        F.when(delivered.isNotNull(), delivered > estimated).alias("delivered_late"),
        F.coalesce(F.col("n_items"), zero).alias("n_items"),
        F.coalesce(F.col("items_value"), zero).cast("decimal(12,2)").alias("items_value"),
        F.coalesce(F.col("freight_value"), zero).cast("decimal(12,2)").alias("freight_value"),
        F.coalesce(F.col("payment_value"), zero).cast("decimal(12,2)").alias("payment_value"),
    ))


def build_fact_items(orders, items, snapshots):
    o = attach_customer(orders, snapshots).select(
        "order_id", "customer_unique_id", local_date("order_purchase_timestamp").alias("order_date"))
    return items.join(o, "order_id").select(
        "order_id", F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id", "seller_id", "customer_unique_id", "order_date",
        F.col("price").cast("decimal(10,2)").alias("price"),
        F.col("freight_value").cast("decimal(10,2)").alias("freight_value"))


def build_dim_product(products, translation):
    p = products.join(translation, "product_category_name", "left")
    return p.select(
        "product_id",
        F.col("product_category_name").alias("category_pt"),
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name"),
                   F.lit("unknown")).alias("category_en"),
        F.col("product_name_lenght").cast("int").alias("name_length"),          # sic: Olist spells it "lenght"
        F.col("product_description_lenght").cast("int").alias("description_length"),
        F.col("product_photos_qty").cast("int").alias("photos_qty"),
        F.col("product_weight_g").cast("int").alias("weight_g"),
        F.col("product_length_cm").cast("int").alias("length_cm"),
        F.col("product_height_cm").cast("int").alias("height_cm"),
        F.col("product_width_cm").cast("int").alias("width_cm"))


def build_dim_seller(sellers):
    return sellers.select(
        "seller_id",
        F.lpad(F.col("seller_zip_code_prefix").cast("string"), 5, "0").alias("zip"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"))