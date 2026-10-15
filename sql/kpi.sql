-- 1 FUNNEL: share of sessions that reached each stage (reached-semantics), per day
SELECT session_date,
       count(*)                                         AS sessions,
       sum(viewed::int)                                 AS viewed,
       sum(carted::int)                                 AS carted,
       sum(checkout_started::int)                       AS checkout_started,
       sum(purchased::int)                              AS purchased,
       round(100.0 * sum(carted::int) / nullif(sum(viewed::int),0), 1)           AS view_to_cart_pct,
       round(100.0 * sum(checkout_started::int) / nullif(sum(carted::int),0), 1) AS cart_to_checkout_pct,
       round(100.0 * sum(purchased::int) / nullif(sum(checkout_started::int),0), 1) AS checkout_to_purchase_pct,
       round(100.0 * sum(purchased::int) / nullif(count(*),0), 2)                AS session_conversion_pct
  FROM fact_sessions GROUP BY 1 ORDER BY 1;

-- 2 REVENUE by category and customer state (item price only; canceled and unavailable orders excluded)
SELECT coalesce(p.category_en, 'unknown') AS category,
       c.state                            AS customer_state,
       count(DISTINCT i.order_id)         AS orders,
       sum(i.price)                       AS revenue,
       sum(i.freight_value)               AS freight
  FROM fact_order_items i
  JOIN fact_orders   o ON o.order_id = i.order_id
  JOIN dim_product   p ON p.product_id = i.product_id
  JOIN dim_customer  c ON c.customer_sk = i.customer_sk
 WHERE o.order_status NOT IN ('canceled', 'unavailable')
 GROUP BY 1, 2 ORDER BY revenue DESC;

-- 3 ON-TIME DELIVERY: delivered orders that have a delivery date; on time = not late
SELECT order_date,
       count(*)                                              AS delivered_orders,
       sum((NOT delivered_late)::int)                        AS on_time,
       round(100.0 * sum((NOT delivered_late)::int) / count(*), 1) AS on_time_pct
  FROM fact_orders
 WHERE order_status = 'delivered' AND delivered_customer_ts IS NOT NULL
 GROUP BY 1 ORDER BY 1;

-- 4 SELLERS: revenue and late rate for sellers with at least 5 orders (top 10 and bottom 10 by revenue)
WITH s AS (
  SELECT i.seller_id, count(DISTINCT i.order_id) AS orders, sum(i.price) AS revenue,
         round(100.0 * count(DISTINCT i.order_id) FILTER (WHERE o.delivered_late)
               / nullif(count(DISTINCT i.order_id) FILTER (WHERE o.delivered_late IS NOT NULL), 0), 1) AS late_pct
    FROM fact_order_items i JOIN fact_orders o ON o.order_id = i.order_id
   WHERE o.order_status NOT IN ('canceled', 'unavailable')
   GROUP BY 1 HAVING count(DISTINCT i.order_id) >= 5)
(SELECT 'top' AS bucket, * FROM s ORDER BY revenue DESC LIMIT 10)
UNION ALL
(SELECT 'bottom', * FROM s ORDER BY revenue ASC LIMIT 10);