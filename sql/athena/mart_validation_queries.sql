-- ============================================================
-- Restaurant Business Insights - Mart Validation Queries
-- Athena database: restaurant_analytics
-- Partition: ingest_date = '2026-05-27'
-- ============================================================


-- 1. Confirm available mart tables
SHOW TABLES IN restaurant_analytics;


-- 2. Validate daily_sales totals
SELECT
    SUM(order_count) AS total_orders,
    SUM(net_revenue) AS total_net_revenue
FROM restaurant_analytics.daily_sales
WHERE ingest_date = '2026-05-27';


-- 3. Validate restaurant_daily_sales totals
SELECT
    SUM(order_count) AS total_orders,
    SUM(net_revenue) AS total_net_revenue
FROM restaurant_analytics.restaurant_daily_sales
WHERE ingest_date = '2026-05-27';


-- 4. Validate customer CLV snapshot totals
SELECT
    COUNT(*) AS customer_count,
    SUM(lifetime_net_revenue) AS total_lifetime_net_revenue
FROM restaurant_analytics.customer_clv_snapshot
WHERE ingest_date = '2026-05-27';


-- 5. Validate item_sales totals
SELECT
    COUNT(*) AS item_count,
    SUM(line_item_count) AS total_line_items,
    SUM(total_item_quantity) AS total_item_quantity,
    SUM(net_revenue) AS total_net_revenue
FROM restaurant_analytics.item_sales
WHERE ingest_date = '2026-05-27';


-- 6. Validate restaurant_item_sales totals
SELECT
    COUNT(*) AS restaurant_item_count,
    SUM(line_item_count) AS total_line_items,
    SUM(total_item_quantity) AS total_item_quantity,
    SUM(net_revenue) AS total_net_revenue
FROM restaurant_analytics.restaurant_item_sales
WHERE ingest_date = '2026-05-27';


-- 7. Validate option_sales totals
SELECT
    COUNT(*) AS option_count,
    SUM(option_row_count) AS total_option_rows,
    SUM(total_option_quantity) AS total_option_quantity,
    SUM(option_revenue) AS total_option_revenue,
    SUM(discount_amount) AS total_discount_amount
FROM restaurant_analytics.option_sales
WHERE ingest_date = '2026-05-27';


-- 8. Validate customer_rfm grain
SELECT
    COUNT(*) AS rfm_row_count,
    COUNT(DISTINCT user_id) AS distinct_customer_count,
    SUM(lifetime_net_revenue) AS total_lifetime_net_revenue
FROM restaurant_analytics.customer_rfm
WHERE ingest_date = '2026-05-27';