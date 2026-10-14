-- sql/postgres/20_load_facts.sql
-- Point-in-time lookup: the customer version that was valid on the order date.
-- Re-running an old day resolves to the SAME version even after the customer later moved.

MERGE INTO fact_orders f
USING (
    SELECT s.*, c.customer_sk
      FROM stg_orders s
      LEFT JOIN dim_customer c
        ON c.customer_unique_id = s.customer_unique_id
       AND s.order_date >= c.valid_from
       AND (c.valid_to IS NULL OR s.order_date < c.valid_to)
) s ON f.order_id = s.order_id
WHEN MATCHED AND (f.customer_sk, f.order_status, f.purchase_ts, f.approved_ts, f.delivered_carrier_ts,
                  f.delivered_customer_ts, f.estimated_delivery_ts, f.delivered_late, f.n_items,
                  f.items_value, f.freight_value, f.payment_value)
     IS DISTINCT FROM (s.customer_sk, s.order_status, s.purchase_ts, s.approved_ts, s.delivered_carrier_ts,
                  s.delivered_customer_ts, s.estimated_delivery_ts, s.delivered_late, s.n_items,
                  s.items_value, s.freight_value, s.payment_value) THEN
  UPDATE SET customer_sk = s.customer_sk, order_status = s.order_status, purchase_ts = s.purchase_ts,
             approved_ts = s.approved_ts, delivered_carrier_ts = s.delivered_carrier_ts,
             delivered_customer_ts = s.delivered_customer_ts, estimated_delivery_ts = s.estimated_delivery_ts,
             delivered_late = s.delivered_late, n_items = s.n_items, items_value = s.items_value,
             freight_value = s.freight_value, payment_value = s.payment_value
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_sk, customer_unique_id, order_date, order_status, purchase_ts, approved_ts,
          delivered_carrier_ts, delivered_customer_ts, estimated_delivery_ts, delivered_late,
          n_items, items_value, freight_value, payment_value)
  VALUES (s.order_id, s.customer_sk, s.customer_unique_id, s.order_date, s.order_status, s.purchase_ts,
          s.approved_ts, s.delivered_carrier_ts, s.delivered_customer_ts, s.estimated_delivery_ts,
          s.delivered_late, s.n_items, s.items_value, s.freight_value, s.payment_value);

MERGE INTO fact_order_items f
USING (
    SELECT s.*, c.customer_sk
      FROM stg_order_items s
      LEFT JOIN dim_customer c
        ON c.customer_unique_id = s.customer_unique_id
       AND s.order_date >= c.valid_from
       AND (c.valid_to IS NULL OR s.order_date < c.valid_to)
) s ON f.order_id = s.order_id AND f.order_item_id = s.order_item_id
WHEN MATCHED AND (f.product_id, f.seller_id, f.customer_sk, f.price, f.freight_value)
     IS DISTINCT FROM (s.product_id, s.seller_id, s.customer_sk, s.price, s.freight_value) THEN
  UPDATE SET product_id = s.product_id, seller_id = s.seller_id, customer_sk = s.customer_sk,
             price = s.price, freight_value = s.freight_value
WHEN NOT MATCHED THEN
  INSERT (order_id, order_item_id, product_id, seller_id, customer_sk, customer_unique_id,
          order_date, price, freight_value)
  VALUES (s.order_id, s.order_item_id, s.product_id, s.seller_id, s.customer_sk, s.customer_unique_id,
          s.order_date, s.price, s.freight_value);

-- Sessions arrive as complete, recomputed rows, so a matched row is replaced wholesale.
MERGE INTO fact_sessions f
USING stg_sessions s ON f.session_id = s.session_id
WHEN MATCHED AND (f.n_events, f.session_start, f.session_end, f.furthest_stage, f.viewed, f.carted,
                  f.checkout_started, f.purchased)
     IS DISTINCT FROM (s.n_events, s.session_start, s.session_end, s.furthest_stage, s.viewed, s.carted,
                  s.checkout_started, s.purchased) THEN
  UPDATE SET customer_unique_id = s.customer_unique_id, session_date = s.session_date, device = s.device,
             session_start = s.session_start, session_end = s.session_end, n_events = s.n_events,
             furthest_stage = s.furthest_stage, viewed = s.viewed, carted = s.carted,
             checkout_started = s.checkout_started, purchased = s.purchased
WHEN NOT MATCHED THEN
  INSERT (session_id, customer_unique_id, session_date, device, session_start, session_end, n_events,
          furthest_stage, viewed, carted, checkout_started, purchased)
  VALUES (s.session_id, s.customer_unique_id, s.session_date, s.device, s.session_start, s.session_end,
          s.n_events, s.furthest_stage, s.viewed, s.carted, s.checkout_started, s.purchased);