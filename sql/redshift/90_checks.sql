-- sql/redshift/90_checks.sql
-- Redshift enforces no constraints, so this is the ONLY thing standing between a bad load and the tables.
-- Every row must show violations = 0. (SUM(CASE ...) instead of FILTER; no tuple comparisons.)
SELECT 'duplicate orders' AS check_name, COUNT(*) AS violations FROM (
    SELECT order_id FROM fact_orders GROUP BY order_id HAVING COUNT(*) > 1) x
UNION ALL
SELECT 'duplicate order items', COUNT(*) FROM (
    SELECT order_id, order_item_id FROM fact_order_items GROUP BY order_id, order_item_id HAVING COUNT(*) > 1) x
UNION ALL
SELECT 'duplicate sessions', COUNT(*) FROM (
    SELECT session_id FROM fact_sessions GROUP BY session_id HAVING COUNT(*) > 1) x
UNION ALL
SELECT 'customer without exactly one current row', COUNT(*) FROM (
    SELECT customer_unique_id FROM dim_customer GROUP BY customer_unique_id
    HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1) x
UNION ALL
SELECT 'overlapping validity ranges', COUNT(*) FROM dim_customer a JOIN dim_customer b
    ON a.customer_unique_id = b.customer_unique_id AND a.customer_sk < b.customer_sk
   AND a.valid_from < COALESCE(b.valid_to, DATE '9999-12-31')
   AND b.valid_from < COALESCE(a.valid_to, DATE '9999-12-31')
UNION ALL
SELECT 'order items without an order', COUNT(*) FROM fact_order_items i
    LEFT JOIN fact_orders o ON o.order_id = i.order_id WHERE o.order_id IS NULL
UNION ALL
SELECT 'order total <> sum of its items', COUNT(*) FROM fact_orders o JOIN (
    SELECT order_id, COUNT(*) AS n, SUM(price) AS p, SUM(freight_value) AS f
      FROM fact_order_items GROUP BY order_id) i ON i.order_id = o.order_id
    WHERE o.n_items <> i.n OR o.items_value <> i.p OR o.freight_value <> i.f
UNION ALL
SELECT 'fact rows with no customer version', COUNT(*) FROM fact_orders WHERE customer_sk IS NULL
UNION ALL
SELECT 'order items missing product or seller', COUNT(*) FROM fact_order_items i
    LEFT JOIN dim_product p ON p.product_id = i.product_id
    LEFT JOIN dim_seller s ON s.seller_id = i.seller_id
    WHERE p.product_id IS NULL OR s.seller_id IS NULL;