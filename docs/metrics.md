# KPI definitions

All KPIs are computed on the star schema (`fact_*`, `dim_*`). Business day = Brazil calendar day (America/Sao_Paulo).
The same SQL runs on Postgres (local warehouse, feeds Metabase) and, with the same shape, on Redshift.

## 1. Conversion funnel
- **Source:** `fact_sessions`. One row per session; `viewed`, `carted`, `checkout_started`, `purchased` are "reached" flags.
- **Reached semantics:** a session that purchased also counts as carted and checked out, even if an intermediate event was quarantined. This keeps every stage count monotone (never higher than the stage before).
- **Rates:** stage-to-stage (cart / view, checkout / cart, purchase / checkout) and overall session conversion (purchased / all sessions).
- **Not included:** bots, internal traffic (none generated). Sessions are attributed to the day they started.

## 2. Revenue by category and customer state
- **Source:** `fact_order_items` joined to `fact_orders` (status), `dim_product` (category), `dim_customer` (state).
- **Revenue** = sum of item `price`. Freight is reported separately, never added in.
- **Excluded:** orders with status `canceled` or `unavailable`. All other statuses count, including not-yet-delivered ones (it is booked revenue, not recognized revenue).
- **State is point-in-time:** the customer version that was valid on the order date, so a later move never rewrites history.
- **Unknown category:** products with no category are shown as `unknown` rather than dropped.

## 3. On-time delivery rate
- **Source:** `fact_orders`.
- **Population:** `order_status = 'delivered'` AND `delivered_customer_ts IS NOT NULL`. (Delivered orders without a delivery date were rejected by the quality gate: they cannot be judged.)
- **On time** = `NOT delivered_late`, where late means delivered after the estimated delivery date.
- **Grouped by order date**, not delivery date, so a day's number can still move while its orders are in flight.

## 4. Top and bottom sellers
- **Source:** `fact_order_items` joined to `fact_orders`.
- **Population:** sellers with at least 5 distinct non-canceled orders in the window (a seller with 1 order tells you nothing).
- **Ranking:** by item revenue. Late rate shown beside it so a high-revenue seller with poor delivery is visible.