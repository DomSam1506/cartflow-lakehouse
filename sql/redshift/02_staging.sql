-- sql/redshift/02_staging.sql   (safe to run repeatedly)
-- Column ORDER must equal the gold Parquet column order: COPY from Parquet maps by POSITION.
CREATE TABLE IF NOT EXISTS stg_customer (
    customer_unique_id VARCHAR(64) NOT NULL, zip VARCHAR(10), city VARCHAR(100), state CHAR(2),
    effective_date DATE NOT NULL
);
CREATE TABLE IF NOT EXISTS stg_product (LIKE dim_product);
CREATE TABLE IF NOT EXISTS stg_seller  (LIKE dim_seller);
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR(64) NOT NULL,
    customer_unique_id VARCHAR(64) NOT NULL, order_date DATE NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    purchase_ts TIMESTAMP NOT NULL, approved_ts TIMESTAMP, delivered_carrier_ts TIMESTAMP,
    delivered_customer_ts TIMESTAMP, estimated_delivery_ts TIMESTAMP, delivered_late BOOLEAN,
    n_items INT NOT NULL, items_value NUMERIC(12,2) NOT NULL,
    freight_value NUMERIC(12,2) NOT NULL, payment_value NUMERIC(12,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS stg_order_items (
    order_id VARCHAR(64) NOT NULL, order_item_id INT NOT NULL,
    product_id VARCHAR(64) NOT NULL, seller_id VARCHAR(64) NOT NULL,
    customer_unique_id VARCHAR(64) NOT NULL, order_date DATE NOT NULL,
    price NUMERIC(10,2) NOT NULL, freight_value NUMERIC(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS stg_sessions (
    session_id VARCHAR(64) NOT NULL, customer_unique_id VARCHAR(64), session_date DATE NOT NULL,
    device VARCHAR(20), session_start TIMESTAMP NOT NULL, session_end TIMESTAMP NOT NULL,
    n_events INT NOT NULL, furthest_stage VARCHAR(20) NOT NULL,
    viewed BOOLEAN NOT NULL, carted BOOLEAN NOT NULL,
    checkout_started BOOLEAN NOT NULL, purchased BOOLEAN NOT NULL
);