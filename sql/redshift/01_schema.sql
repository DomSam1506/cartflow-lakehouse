-- sql/redshift/01_schema.sql   (safe to run repeatedly)
-- Redshift notes: PRIMARY KEY / UNIQUE are informational only (NOT enforced), there are no CHECK
-- constraints or partial indexes, so the load script's invariant checks are the real guard.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT NOT NULL,
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    day             INT NOT NULL,
    day_of_week     INT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_black_friday BOOLEAN NOT NULL,
    PRIMARY KEY (date_key)
) DISTSTYLE ALL SORTKEY (full_date);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk        BIGINT IDENTITY(1,1),
    customer_unique_id VARCHAR(64) NOT NULL,
    zip                VARCHAR(10),
    city               VARCHAR(100),
    state              CHAR(2),
    valid_from         DATE NOT NULL,
    valid_to           DATE,
    is_current         BOOLEAN NOT NULL,
    PRIMARY KEY (customer_sk)
) DISTSTYLE ALL SORTKEY (customer_unique_id);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id            VARCHAR(64) NOT NULL,
    category_pt           VARCHAR(100),
    category_en           VARCHAR(100),
    name_length           INT,
    description_length    INT,
    photos_qty            INT,
    weight_g              INT,
    length_cm             INT,
    height_cm             INT,
    width_cm              INT,
    PRIMARY KEY (product_id)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id VARCHAR(64) NOT NULL,
    zip       VARCHAR(10),
    city      VARCHAR(100),
    state     CHAR(2),
    PRIMARY KEY (seller_id)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                  VARCHAR(64) NOT NULL,
    customer_sk               BIGINT,
    customer_unique_id        VARCHAR(64) NOT NULL,
    order_date                DATE NOT NULL,
    order_status              VARCHAR(20) NOT NULL,
    purchase_ts               TIMESTAMP NOT NULL,
    approved_ts               TIMESTAMP,
    delivered_carrier_ts      TIMESTAMP,
    delivered_customer_ts     TIMESTAMP,
    estimated_delivery_ts     TIMESTAMP,
    delivered_late            BOOLEAN,
    n_items                   INT NOT NULL,
    items_value               NUMERIC(12,2) NOT NULL,
    freight_value             NUMERIC(12,2) NOT NULL,
    payment_value             NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id)
) DISTKEY (order_id) SORTKEY (order_date);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id           VARCHAR(64) NOT NULL,
    order_item_id      INT NOT NULL,
    product_id         VARCHAR(64) NOT NULL,
    seller_id          VARCHAR(64) NOT NULL,
    customer_sk        BIGINT,
    customer_unique_id VARCHAR(64) NOT NULL,
    order_date         DATE NOT NULL,
    price              NUMERIC(10,2) NOT NULL,
    freight_value      NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
) DISTKEY (order_id) SORTKEY (order_date);

CREATE TABLE IF NOT EXISTS fact_sessions (
    session_id         VARCHAR(64) NOT NULL,
    customer_unique_id VARCHAR(64),
    session_date       DATE NOT NULL,
    device             VARCHAR(20),
    session_start      TIMESTAMP NOT NULL,
    session_end        TIMESTAMP NOT NULL,
    n_events           INT NOT NULL,
    furthest_stage     VARCHAR(20) NOT NULL,
    viewed             BOOLEAN NOT NULL,
    carted             BOOLEAN NOT NULL,
    checkout_started   BOOLEAN NOT NULL,
    purchased          BOOLEAN NOT NULL,
    PRIMARY KEY (session_id)
) DISTKEY (session_id) SORTKEY (session_date);