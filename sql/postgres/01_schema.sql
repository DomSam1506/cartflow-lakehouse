-- sql/postgres/01_schema.sql   (safe to run repeatedly)

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,            -- 20171124
    full_date       DATE NOT NULL UNIQUE,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      TEXT NOT NULL,
    day             INT NOT NULL,
    day_of_week     INT NOT NULL,               -- 1 = Monday ... 7 = Sunday
    day_name        TEXT NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_black_friday BOOLEAN NOT NULL
);

-- Covers the whole Olist range, so any order date resolves. Re-running adds nothing.
INSERT INTO dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d::date,
       extract(year FROM d)::int, extract(quarter FROM d)::int, extract(month FROM d)::int,
       trim(to_char(d, 'Month')), extract(day FROM d)::int,
       extract(isodow FROM d)::int, trim(to_char(d, 'Day')),
       extract(isodow FROM d) IN (6, 7),
       d::date = DATE '2017-11-24'
FROM generate_series(DATE '2016-09-01', DATE '2018-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- Type 2 dimension: one row per customer per address period.
-- Validity is half-open: valid_from <= date < valid_to (valid_to NULL = still current).
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_unique_id VARCHAR(64) NOT NULL,
    zip                VARCHAR(10),
    city               VARCHAR(100),
    state              CHAR(2),
    valid_from         DATE NOT NULL,
    valid_to           DATE,
    is_current         BOOLEAN NOT NULL,
    CONSTRAINT valid_range CHECK (valid_to IS NULL OR valid_to > valid_from),
    CONSTRAINT current_has_no_end CHECK (is_current = (valid_to IS NULL)),
    CONSTRAINT one_row_per_start UNIQUE (customer_unique_id, valid_from)
);
-- The database itself refuses a second current row for the same person.
CREATE UNIQUE INDEX IF NOT EXISTS one_current_per_customer
    ON dim_customer (customer_unique_id) WHERE is_current;

-- Type 1 dimensions: overwrite in place, no history.
CREATE TABLE IF NOT EXISTS dim_product (
    product_id            VARCHAR(64) PRIMARY KEY,
    category_pt           VARCHAR(100),
    category_en           VARCHAR(100),
    name_length           INT,
    description_length    INT,
    photos_qty            INT,
    weight_g              INT,
    length_cm             INT,
    height_cm             INT,
    width_cm              INT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id VARCHAR(64) PRIMARY KEY,
    zip       VARCHAR(10),
    city      VARCHAR(100),
    state     CHAR(2)
);

-- Grain: one row per order.
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                  VARCHAR(64) PRIMARY KEY,
    customer_sk               BIGINT REFERENCES dim_customer (customer_sk),
    customer_unique_id        VARCHAR(64) NOT NULL,
    order_date                DATE NOT NULL REFERENCES dim_date (full_date),
    order_status              VARCHAR(20) NOT NULL,
    purchase_ts               TIMESTAMP NOT NULL,           -- UTC
    approved_ts               TIMESTAMP,
    delivered_carrier_ts      TIMESTAMP,
    delivered_customer_ts     TIMESTAMP,
    estimated_delivery_ts     TIMESTAMP,
    delivered_late            BOOLEAN,                      -- NULL until delivered
    n_items                   INT NOT NULL,
    items_value               NUMERIC(12,2) NOT NULL,
    freight_value             NUMERIC(12,2) NOT NULL,
    payment_value             NUMERIC(12,2) NOT NULL
);

-- Grain: one row per order item.
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id           VARCHAR(64) NOT NULL,
    order_item_id      INT NOT NULL,
    product_id         VARCHAR(64) NOT NULL,
    seller_id          VARCHAR(64) NOT NULL,
    customer_sk        BIGINT REFERENCES dim_customer (customer_sk),
    customer_unique_id VARCHAR(64) NOT NULL,
    order_date         DATE NOT NULL REFERENCES dim_date (full_date),
    price              NUMERIC(10,2) NOT NULL,
    freight_value      NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

-- Grain: one row per browsing session.
CREATE TABLE IF NOT EXISTS fact_sessions (
    session_id         VARCHAR(64) PRIMARY KEY,
    customer_unique_id VARCHAR(64),
    session_date       DATE NOT NULL REFERENCES dim_date (full_date),
    device             VARCHAR(20),
    session_start      TIMESTAMP NOT NULL,
    session_end        TIMESTAMP NOT NULL,
    n_events           INT NOT NULL,
    furthest_stage     VARCHAR(20) NOT NULL,
    viewed             BOOLEAN NOT NULL,
    carted             BOOLEAN NOT NULL,
    checkout_started   BOOLEAN NOT NULL,
    purchased          BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_fact_orders_date  ON fact_orders (order_date);
CREATE INDEX IF NOT EXISTS ix_fact_items_date   ON fact_order_items (order_date);
CREATE INDEX IF NOT EXISTS ix_fact_items_prod   ON fact_order_items (product_id);
CREATE INDEX IF NOT EXISTS ix_fact_sessions_date ON fact_sessions (session_date);