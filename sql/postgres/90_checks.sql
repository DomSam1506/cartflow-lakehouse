-- sql/postgres/90_checks.sql
-- Each row is one invariant. violations must be 0 for every row.
SELECT 'duplicate order items' AS check_name, count(*) AS violations FROM (
    SELECT order_id, order_item_id FROM fact_order_items GROUP BY 1, 2 HAVING count(*) > 1) x
UNION ALL
SELECT 'customer without exactly one current row', count(*) FROM (
    SELECT customer_unique_id FROM dim_customer GROUP BY 1
    HAVING count(*) FILTER (WHERE is_current) <> 1) x
UNION ALL
SELECT 'overlapping validity ranges', count(*) FROM dim_customer a JOIN dim_customer b
    ON a.customer_unique_id = b.customer_unique_id AND a.customer_sk < b.customer_sk
   AND a.valid_from < COALESCE(b.valid_to, DATE '9999-12-31')
   AND b.valid_from < COALESCE(a.valid_to, DATE '9999-12-31')
UNION ALL
SELECT 'order items without an order', count(*) FROM fact_order_items i
    LEFT JOIN fact_orders o USING (order_id) WHERE o.order_id IS NULL
UNION ALL
SELECT 'order total <> sum of its items', count(*) FROM fact_orders o JOIN (
    SELECT order_id, count(*) n, sum(price) p, sum(freight_value) f
      FROM fact_order_items GROUP BY 1) i USING (order_id)
    WHERE o.n_items <> i.n OR o.items_value <> i.p OR o.freight_value <> i.f
UNION ALL
SELECT 'fact rows with no customer version', count(*) FROM fact_orders WHERE customer_sk IS NULL
UNION ALL
SELECT 'fact points at wrong customer version', count(*) FROM fact_orders o
    JOIN dim_customer c USING (customer_sk)
    WHERE NOT (o.order_date >= c.valid_from AND (c.valid_to IS NULL OR o.order_date < c.valid_to))
UNION ALL
SELECT 'order items missing product or seller', count(*) FROM fact_order_items i
    LEFT JOIN dim_product p USING (product_id) LEFT JOIN dim_seller s USING (seller_id)
    WHERE p.product_id IS NULL OR s.seller_id IS NULL;