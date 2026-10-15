-- sql/redshift/10_load_dimensions.sql
-- Redshift MERGE has no "WHEN MATCHED AND <condition>", so matched rows are always updated.
-- That is still idempotent (same values written again). Source must have ONE row per key.
MERGE INTO dim_product
USING stg_product s ON dim_product.product_id = s.product_id
WHEN MATCHED THEN
  UPDATE SET category_pt = s.category_pt, category_en = s.category_en, name_length = s.name_length,
             description_length = s.description_length, photos_qty = s.photos_qty,
             weight_g = s.weight_g, length_cm = s.length_cm, height_cm = s.height_cm, width_cm = s.width_cm
WHEN NOT MATCHED THEN
  INSERT (product_id, category_pt, category_en, name_length, description_length, photos_qty,
          weight_g, length_cm, height_cm, width_cm)
  VALUES (s.product_id, s.category_pt, s.category_en, s.name_length, s.description_length, s.photos_qty,
          s.weight_g, s.length_cm, s.height_cm, s.width_cm);

MERGE INTO dim_seller
USING stg_seller s ON dim_seller.seller_id = s.seller_id
WHEN MATCHED THEN
  UPDATE SET zip = s.zip, city = s.city, state = s.state
WHEN NOT MATCHED THEN
  INSERT (seller_id, zip, city, state) VALUES (s.seller_id, s.zip, s.city, s.state);

-- SCD2 part 1: close the current row when the address changed.
-- IS DISTINCT FROM is not documented for Redshift, so NULL-safe comparison is spelled out with COALESCE.
UPDATE dim_customer
   SET valid_to = s.effective_date, is_current = FALSE
  FROM stg_customer s
 WHERE dim_customer.customer_unique_id = s.customer_unique_id
   AND dim_customer.is_current
   AND s.effective_date > dim_customer.valid_from
   AND (COALESCE(dim_customer.zip, '~') <> COALESCE(s.zip, '~')
     OR COALESCE(dim_customer.city, '~') <> COALESCE(s.city, '~')
     OR COALESCE(dim_customer.state, '~') <> COALESCE(s.state, '~'));

-- Part 2: open a current row for new customers and for the ones just closed.
INSERT INTO dim_customer (customer_unique_id, zip, city, state, valid_from, valid_to, is_current)
SELECT s.customer_unique_id, s.zip, s.city, s.state, s.effective_date, NULL, TRUE
  FROM stg_customer s
  LEFT JOIN dim_customer d
         ON d.customer_unique_id = s.customer_unique_id AND d.is_current
 WHERE d.customer_unique_id IS NULL;