-- sql/postgres/10_load_dimensions.sql
-- Runs inside ONE transaction opened by load_day.py. Dimensions load BEFORE facts.

-- Type 1: overwrite changed attributes, insert new ones.
MERGE INTO dim_product d
USING stg_product s ON d.product_id = s.product_id
WHEN MATCHED AND (d.category_pt, d.category_en, d.name_length, d.description_length, d.photos_qty,
                  d.weight_g, d.length_cm, d.height_cm, d.width_cm)
     IS DISTINCT FROM (s.category_pt, s.category_en, s.name_length, s.description_length, s.photos_qty,
                  s.weight_g, s.length_cm, s.height_cm, s.width_cm) THEN
  UPDATE SET category_pt = s.category_pt, category_en = s.category_en, name_length = s.name_length,
             description_length = s.description_length, photos_qty = s.photos_qty,
             weight_g = s.weight_g, length_cm = s.length_cm, height_cm = s.height_cm, width_cm = s.width_cm
WHEN NOT MATCHED THEN
  INSERT (product_id, category_pt, category_en, name_length, description_length, photos_qty,
          weight_g, length_cm, height_cm, width_cm)
  VALUES (s.product_id, s.category_pt, s.category_en, s.name_length, s.description_length, s.photos_qty,
          s.weight_g, s.length_cm, s.height_cm, s.width_cm);

MERGE INTO dim_seller d
USING stg_seller s ON d.seller_id = s.seller_id
WHEN MATCHED AND (d.zip, d.city, d.state) IS DISTINCT FROM (s.zip, s.city, s.state) THEN
  UPDATE SET zip = s.zip, city = s.city, state = s.state
WHEN NOT MATCHED THEN
  INSERT (seller_id, zip, city, state) VALUES (s.seller_id, s.zip, s.city, s.state);

-- Type 2 customer, part 1: close the current row when the address changed.
-- effective_date > valid_from means an old or repeated snapshot can never rewrite history.
UPDATE dim_customer d
   SET valid_to = s.effective_date, is_current = FALSE
  FROM stg_customer s
 WHERE d.customer_unique_id = s.customer_unique_id
   AND d.is_current
   AND s.effective_date > d.valid_from
   AND (d.zip, d.city, d.state) IS DISTINCT FROM (s.zip, s.city, s.state);

-- Part 2: open a current row for brand-new customers AND for the ones just closed.
INSERT INTO dim_customer (customer_unique_id, zip, city, state, valid_from, valid_to, is_current)
SELECT s.customer_unique_id, s.zip, s.city, s.state, s.effective_date, NULL, TRUE
  FROM stg_customer s
  LEFT JOIN dim_customer d
         ON d.customer_unique_id = s.customer_unique_id AND d.is_current
 WHERE d.customer_unique_id IS NULL;