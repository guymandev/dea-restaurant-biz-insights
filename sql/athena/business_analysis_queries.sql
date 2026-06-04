-- Top 10 items by net revenue
SELECT
    item_category,
    item_name,
    net_revenue,
    total_item_quantity,
    line_item_count,
    avg_net_revenue_per_line
FROM restaurant_analytics.item_sales
WHERE ingest_date = '2026-05-27'
ORDER BY net_revenue DESC
LIMIT 10;


-- Top 10 restaurants by net revenue
SELECT
    restaurant_id,
    SUM(order_count) AS total_orders,
    SUM(net_revenue) AS total_net_revenue,
    AVG(avg_order_value) AS avg_daily_aov
FROM restaurant_analytics.restaurant_daily_sales
WHERE ingest_date = '2026-05-27'
GROUP BY restaurant_id
ORDER BY total_net_revenue DESC
LIMIT 10;


-- Daily revenue trend
SELECT
    order_date,
    order_count,
    customer_count,
    net_revenue,
    avg_order_value
FROM restaurant_analytics.daily_sales
WHERE ingest_date = '2026-05-27'
ORDER BY order_date;


-- Customer count by RFM segment
SELECT
    rfm_segment,
    COUNT(*) AS customer_count,
    SUM(lifetime_net_revenue) AS segment_lifetime_net_revenue
FROM restaurant_analytics.customer_rfm
WHERE ingest_date = '2026-05-27'
GROUP BY rfm_segment
ORDER BY segment_lifetime_net_revenue DESC;


-- Top options by revenue
SELECT
    option_group_name,
    option_name,
    option_revenue,
    total_option_quantity,
    option_row_count
FROM restaurant_analytics.option_sales
WHERE ingest_date = '2026-05-27'
ORDER BY option_revenue DESC
LIMIT 10;

-- Top 10 restaurant-item combinations by net revenue
SELECT
    restaurant_id,
    item_category,
    item_name,
    net_revenue,
    total_item_quantity,
    line_item_count,
    avg_net_revenue_per_line,
    avg_net_revenue_per_unit
FROM restaurant_analytics.restaurant_item_sales
WHERE ingest_date = '2026-05-27'
ORDER BY net_revenue DESC
LIMIT 10;