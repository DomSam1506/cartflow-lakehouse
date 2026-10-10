# Olist Data Profile

Profiled with DuckDB directly on the raw CSVs (`scripts/profile_full.py`, Day 2).
Source: Olist Brazilian E-Commerce dataset on Kaggle (CC BY-NC-SA 4.0).

**Summary:** the dataset is large enough and relational enough for the whole design. It is
also unusually clean (it is a curated release), so the data-quality layer will catch few real
defects. The event generator injects controlled faults to exercise it (see section 7).

## 1. Tables, row counts and grain

| Table | Rows | Grain and notes |
|---|---|---|
| orders | 99,441 | One row per order. `order_id` is unique. |
| customers | 99,441 | One row per **order** (`customer_id`). 96,096 distinct real people (`customer_unique_id`). |
| order_items | 112,650 | One row per item. 98,666 orders have items (avg 1.14, max 21 per order). |
| order_payments | 103,886 | Multiple payments per order are normal (max 29: installments and vouchers). |
| order_reviews | 99,224 | 98,410 distinct `review_id`s, 98,673 distinct orders. See section 5. |
| sellers | 3,095 | 1,849 (60%) are in SP. |
| products | 32,951 | |
| geolocation | 1,000,163 | Only 19,015 distinct zip prefixes (about 52 rows per zip). Needs dedup. |
| product_category_name_translation | 71 | Portuguese to English. |

## 2. Date range and replay window

- Orders span 2016-09-04 to 2018-10-17.
- Both ends are unusable: 2016-09 has 4 orders, 2016-10 has 324, 2016-11 has none,
  2016-12 has 1, 2018-09 has 16, 2018-10 has 4. Steady volume (about 6-7K per month) runs from about 2018-01 to 2018-08.
- Busiest day by far: **2017-11-24 (Black Friday), 1,176 orders**. The next busiest
  days are 499 and 403.
- Best 11-day rolling window: 2017-11-24 to 2017-12-04 (4,501 orders). It starts on the spike.
- **Chosen replay window: 2017-11-20 to 2017-11-30** (4,381 orders). It has a build-up,
  the Black Friday spike and the aftermath, which makes a better dashboard story and a
  better test of late-arriving data. It is only 120 orders short of the "best" window.

## 3. Order status

| Status | Orders | % |
|---|---|---|
| delivered | 96,478 | 97.02 |
| shipped | 1,107 | 1.11 |
| canceled | 625 | 0.63 |
| unavailable | 609 | 0.61 |
| invoiced | 314 | 0.32 |
| processing | 301 | 0.30 |
| created | 5 | 0.01 |
| approved | 2 | 0.00 |

Implication: delivery KPIs (on-time rate, delivery days) filter to `delivered`.

## 4. Null rates

| Column | Null % | Expected? |
|---|---|---|
| orders.order_approved_at | 0.16 | Mostly yes (never approved) |
| orders.order_delivered_carrier_date | 1.79 | Yes (not yet shipped) |
| orders.order_delivered_customer_date | 2.98 | Mostly. 2,846 are non-delivered orders. **8 delivered orders have no date (defect).** |
| order_reviews.review_comment_title | 88.34 | Yes, optional field |
| order_reviews.review_comment_message | 58.70 | Yes, optional field |
| products.product_category_name | 1.85 | No, but low (about 610 products) |
| products.product_photos_qty | 1.85 | Same rows as above |
| products.product_weight_g | 0.01 | Negligible |

Delivery-date nulls by status: shipped 1,107/1,107, unavailable 609/609, invoiced 314/314,
processing 301/301, created 5/5, approved 2/2, canceled 619/625 (**6 canceled orders do have a
delivery date**), delivered 8/96,478.

## 5. Data quality findings

| Check | Result | Severity |
|---|---|---|
| Duplicate keys: orders, order_items, payments, products, sellers | 0 | Clean |
| Same `review_id` on different orders | 814 extra rows (no exact duplicate rows) | `review_id` is not unique. Key is (`review_id`, `order_id`) |
| Orders with more than one review | 547 | Keep latest by `review_answer_timestamp` (gold layer decision) |
| Orders with no review | about 768 | Normal |
| Delivered before purchase / approved before purchase | 0 / 0 | Clean |
| Carrier pickup before approval | 1,359 (1.4%) | Warn only (real orders) |
| Delivered to customer before carrier pickup | 23 | Warn only |
| Delivered orders with null delivery date | 8 | **Reject** (quarantine) |
| Zero-value payments | 9 | Warn |
| Zero installments | 2 | Warn |
| Payment type `not_defined` | 3 | Warn |
| Order with no payment | 1 | Warn |
| Negative payments, bad price, negative freight | 0 | Clean |
| Item price range | 0.85 to 6,735.00 | Plausible |
| Review score outside 1-5 | 0 | Clean |
| Items whose order is missing | 0 | Clean referential integrity |
| Orders with no items | 775 | Expected for canceled/unavailable orders. Left join items to orders, never the reverse. |
| Categories missing from translation | 2 (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`), 13 products | Add manual mapping |
| Item total vs payment total mismatch | 380 of 98,665 (0.4%) | Use item totals for revenue |

Review score mix: 5-star 57.8%, 4-star 19.3%, 3-star 8.2%, 2-star 3.2%, 1-star 11.5%.

Business baseline: 8.11% of delivered orders (7,826 of 96,478) arrived after the estimated date.

## 6. Customer identity and SCD2 material

- `customer_id` is per order, so 99,441 customer rows represent 96,096 real people.
- 2,997 people have more than one `customer_id` (repeat buyers).
- **252 people changed zip, city or state between orders.** That is real SCD Type 2 material.
  It is small, so the generator also produces controlled address changes for a stronger demo.
- Geography is concentrated: SP has 41,746 customers (42%) and 1,849 sellers (60%).
  RJ and MG follow. Dashboards should show state-level views with SP separated or labeled.

## 7. How findings drive design

| Finding | Decision |
|---|---|
| `customer_id` is per order | `dim_customer` keyed on `customer_unique_id`, SCD2 on address |
| Orders have 1+ items and 1+ payments | Facts at item grain. Aggregate payments to order level before joining to avoid fan-out |
| 775 orders have no items | Orders are the driver table. Left join items |
| Delivery nulls are expected for non-delivered statuses | DQ rule: delivery date required only when status = `delivered` |
| 8 delivered orders with no date | Quarantine with reason `delivered_missing_delivery_date` |
| 1,359 orders with carrier before approval | Warn and keep. Rejecting 1.4% of real orders would distort revenue |
| `review_id` repeats across orders (814 rows) | Treat (`review_id`, `order_id`) as the key. Never dedupe on `review_id` alone |
| 1M geolocation rows, 19K zips | Aggregate to one centroid per zip prefix before loading `dim_geo` |
| 2 untranslated categories | Maintain a small mapping seed file for them |
| Data sparse outside 2017-2018 | Replay only 2017-11-20 to 2017-11-30 |
| Source is mostly clean | Generator injects labeled synthetic faults (duplicates, nulls, late arrivals, schema drift) so the quarantine path is exercised. The README states clearly that these are synthetic |